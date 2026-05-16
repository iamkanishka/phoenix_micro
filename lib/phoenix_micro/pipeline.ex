defmodule PhoenixMicro.Pipeline do
  @moduledoc """
  A Broadway-backed message-processing pipeline for `PhoenixMicro.Consumer`.

  ## Why Broadway instead of bare Tasks?

  The previous `Consumer.Worker` dispatched messages using `Task.start/1`:
  a fire-and-forget approach that offers no backpressure. Under load, the
  broker can push messages faster than handlers can process them, causing:

  - Unbounded process growth (OOM risk).
  - No visibility into queue depth.
  - All in-flight messages lost on node restart.

  Broadway solves this with a **demand-driven pull model**:

  ```
  Transport broker
        │   push (unbounded)
        ▼
  BroadwayProducer  ←── demand ── Processor pool (N workers)
        │                              │
        │   pull (demand-gated)        ▼
        └─────────────────────── your handle/2
  ```

  Processors only request more messages when they have capacity, creating
  true end-to-end backpressure from handler back to broker.

  ## Pipeline topology

      BroadwayProducer (1 process)
            │
            ▼  (up to :concurrency messages in flight)
      Processor pool  ←── runs middleware + consumer.handle/2
            │
            ▼  (optional — only when :batch_size > 1)
      Batcher pool    ←── runs consumer.handle_batch/3 if defined

  ## Usage

  `PhoenixMicro.Pipeline` is started automatically for every registered
  consumer by `ConsumerManager`. You do not start it directly.

  You configure it via the Consumer DSL:

      defmodule MyApp.PaymentsConsumer do
        use PhoenixMicro.Consumer

        topic       "payments.created"
        concurrency 10        # processor pool size
        batch_size  50        # group messages before handle_batch/3
        batch_timeout 1_000   # flush batch after this many ms even if not full

        def handle(message, _ctx), do: :ok

        # Optional batch handler — only called when batch_size > 1
        def handle_batch(_batch_name, messages, _batch_info, _ctx) do
          Enum.map(messages, fn msg -> Broadway.Message.ack(msg) end)
        end
      end

  ## Telemetry

  In addition to the standard `[:phoenix_micro, :message, *]` events,
  the pipeline emits:

  - `[:phoenix_micro, :pipeline, :demand]`   — processor demand updates
  - `[:phoenix_micro, :pipeline, :enqueued]` — message entered buffer
  - `[:phoenix_micro, :pipeline, :buffer_full]` — buffer at max_demand cap
  - `[:broadway, :processor, :message, :start/stop/exception]` — Broadway's own spans
  """

  use Broadway

  require Logger

  alias PhoenixMicro.{Config, Consumer, Message, Telemetry}
  alias PhoenixMicro.Transport.BroadwayProducer

  # ---------------------------------------------------------------------------
  # Public API — called by ConsumerManager
  # ---------------------------------------------------------------------------

  @doc """
  Starts a supervised Broadway pipeline for the given consumer module.

  Called automatically by `PhoenixMicro.Supervisor.ConsumerManager`.
  """
  @spec start_link(module(), keyword()) :: GenServer.on_start()
  def start_link(consumer_module, opts \\ []) do
    cfg = Consumer.config(consumer_module)

    if not cfg.topic do
      raise ArgumentError,
            "Consumer #{inspect(consumer_module)} must declare a topic via `topic \"...\"`"
    end

    broadway_opts = build_broadway_opts(consumer_module, cfg, opts)
    Broadway.start_link(__MODULE__, broadway_opts)
  end

  @doc """
  Returns a child spec suitable for `DynamicSupervisor`.
  """
  @spec child_spec({module(), keyword()}) :: Supervisor.child_spec()
  def child_spec({consumer_module, opts}) do
    %{
      id: {__MODULE__, consumer_module},
      start: {__MODULE__, :start_link, [consumer_module, opts]},
      restart: :permanent,
      type: :supervisor
    }
  end

  # ---------------------------------------------------------------------------
  # Broadway callbacks
  # ---------------------------------------------------------------------------

  @impl Broadway
  def handle_message(
        _processor,
        %Broadway.Message{data: %Message{} = msg} = broadway_msg,
        context
      ) do
    %{consumer_module: consumer_module} = context

    consumer_cfg = Consumer.config(consumer_module)
    transport_name = consumer_cfg.transport || Config.get(:transport, :memory)

    dispatch_context = %{
      transport: transport_name,
      topic: consumer_cfg.topic,
      attempt: msg.attempt,
      transport_mod: Config.transport_module(transport_name),
      message: msg
    }

    start = System.monotonic_time()

    Telemetry.message_received(msg.topic, %{
      transport: transport_name,
      attempt: msg.attempt,
      pipeline: true
    })

    result =
      try do
        Consumer.dispatch(consumer_module, msg, dispatch_context)
      rescue
        e ->
          Logger.error(
            "[Pipeline] Consumer #{inspect(consumer_module)} raised on #{msg.topic}: #{Exception.message(e)}\n" <>
              Exception.format_stacktrace(__STACKTRACE__)
          )

          {:error, {:exception, e}}
      end

    duration = System.monotonic_time() - start

    case result do
      :ok ->
        Telemetry.message_processed(msg.topic, %{
          transport: transport_name,
          duration: duration,
          consumer: consumer_module
        })

        broadway_msg

      {:error, reason} ->
        Telemetry.message_failed(msg.topic, %{
          transport: transport_name,
          reason: reason,
          attempt: msg.attempt,
          consumer: consumer_module
        })

        broadway_msg
        |> Broadway.Message.failed(reason)
        |> maybe_schedule_retry(msg, consumer_module, consumer_cfg)
    end
  end

  @impl Broadway
  def handle_batch(batcher, broadway_messages, batch_info, context) do
    %{consumer_module: consumer_module} = context

    # If the consumer defines handle_batch/4, delegate to it.
    # Otherwise, return messages as-is (already processed in handle_message/3).
    if function_exported?(consumer_module, :handle_batch, 4) do
      consumer_module.handle_batch(batcher, broadway_messages, batch_info, context)
    else
      broadway_messages
    end
  end

  # ---------------------------------------------------------------------------
  # Private — pipeline configuration
  # ---------------------------------------------------------------------------

  defp build_broadway_opts(consumer_module, cfg, overrides) do
    transport_name = cfg.transport || Config.get(:transport, :memory)
    concurrency = Keyword.get(overrides, :concurrency, cfg.concurrency || 1)
    batch_size = Keyword.get(overrides, :batch_size, Map.get(cfg, :batch_size, 1))
    batch_timeout = Keyword.get(overrides, :batch_timeout, Map.get(cfg, :batch_timeout, 1_000))
    max_demand = Keyword.get(overrides, :max_demand, batch_size * concurrency * 2)

    producer_opts = [
      topic: cfg.topic,
      transport: transport_name,
      queue_group: cfg.queue_group,
      max_demand: max_demand
    ]

    base_opts = [
      name: pipeline_name(consumer_module),
      producer: [
        module: {BroadwayProducer, producer_opts},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [
          concurrency: concurrency,
          max_demand: batch_size
        ]
      ],
      context: %{consumer_module: consumer_module}
    ]

    # Only add batchers if batch_size > 1
    if batch_size > 1 do
      Keyword.put(base_opts, :batchers,
        default: [
          concurrency: max(1, div(concurrency, 4)),
          batch_size: batch_size,
          batch_timeout: batch_timeout
        ]
      )
    else
      base_opts
    end
  end

  @doc false
  # Transformer — called by Broadway between producer and processors.
  # We use it to attach acknowledger info from the BroadwayProducer.
  def transform(%Broadway.Message{} = msg, _opts), do: msg

  defp pipeline_name(consumer_module) do
    Module.safe_concat([PhoenixMicro.Pipeline, consumer_module])
  end

  defp maybe_schedule_retry(broadway_msg, %Message{} = msg, consumer_module, cfg) do
    retry_opts = Config.retry_opts(cfg.retry_opts || [])
    max_attempts = Keyword.get(retry_opts, :max_attempts, 3)

    if msg.attempt < max_attempts do
      delay = PhoenixMicro.Consumer.RetryScheduler.next_delay(msg.attempt, retry_opts)

      Logger.info(
        "[Pipeline] Scheduling retry #{msg.attempt + 1}/#{max_attempts} " <>
          "for #{msg.id} on #{msg.topic} in #{delay}ms"
      )

      # Schedule a re-injection into the pipeline after the backoff delay
      pipeline_pid = self()
      incremented = Message.increment_attempt(msg)

      {:ok, _task} =
        Task.start(fn ->
          Process.sleep(delay)
          send(pipeline_pid, {:retry_inject, incremented})
        end)

      broadway_msg
    else
      # Exhausted — route to DLQ
      dlq_topic = cfg.dlq_topic || "dlq.#{msg.topic}"

      Logger.warning("[Pipeline] Exhausted #{max_attempts} attempts for #{msg.id}, routing to DLQ #{dlq_topic}")

      transport_mod = Config.transport_module(cfg.transport || Config.get(:transport, :memory))

      dlq_msg =
        msg
        |> Message.put_metadata(%{
          dlq_reason: "max_attempts_exceeded",
          original_topic: msg.topic,
          attempts: msg.attempt
        })
        |> Map.put(:topic, dlq_topic)

      transport_mod.publish(dlq_topic, dlq_msg, [])

      Telemetry.message_failed(msg.topic, %{
        reason: :max_attempts_exceeded,
        final: true,
        consumer: consumer_module
      })

      broadway_msg
    end
  end
end
