defmodule PhoenixMicro.Consumer do
  @moduledoc """
  DSL for defining topic consumers with concurrency control, retry logic, and middleware.

  ## Example

      defmodule MyApp.Payments.CreatedConsumer do
        use PhoenixMicro.Consumer

        topic "payments.created"
        concurrency 10
        retry max_attempts: 5, base_delay: 500, max_delay: 30_000

        middleware [
          PhoenixMicro.Middleware.Logger,
          PhoenixMicro.Middleware.Metrics
        ]

        @impl PhoenixMicro.Consumer
        def handle(%PhoenixMicro.Message{} = message, _context) do
          %{"amount" => amount, "currency" => currency} = message.payload
          # ... process ...
          :ok
        end

        # Optional: override per-message error handling
        @impl PhoenixMicro.Consumer
        def handle_error(message, error, _context) do
          Logger.error("Payment processing failed", error: inspect(error))
          {:retry, message}
        end
      end

  ## Behaviour callbacks

  Consumers must implement `handle/2`. `handle_error/3` is optional.

  The return value of `handle/2` controls the ack/nack flow:
  - `:ok`              — Ack the message.
  - `{:ok, _result}`   — Ack.
  - `{:error, reason}` — Nack; triggers retry if attempts remain, DLQ otherwise.
  - `{:retry, message}`— Explicit retry request.
  - `:nack`            — Nack without retry.
  """

  alias PhoenixMicro.{Config, Message}

  @type context :: %{
          transport: atom(),
          topic: String.t(),
          attempt: pos_integer(),
          transport_mod: module(),
          message: Message.t()
        }

  @callback handle(Message.t(), context()) :: :ok | {:ok, term()} | {:error, term()} | :nack
  @callback handle_error(Message.t(), term(), context()) ::
              {:retry, Message.t()} | :nack | :ok

  @optional_callbacks [handle_error: 3]

  # ---------------------------------------------------------------------------
  # Manual ack/nack API
  # Consumers can call these directly instead of using return values.
  # ---------------------------------------------------------------------------

  @doc """
  Manually acknowledges a message from within a handler.

  Use this when you need to ack before the handler returns — for example,
  when kicking off an async process and you want to release the broker slot
  immediately.

      def handle(message, context) do
        PhoenixMicro.Consumer.ack(message, context)
        Task.start(fn -> do_slow_work(message.payload) end)
        :ok
      end
  """
  @spec ack(Message.t(), context()) :: :ok
  def ack(%Message{} = message, %{transport_mod: transport_mod}) do
    transport_mod.ack(message, %{})
  end

  def ack(%Message{} = message, _context) do
    # Fallback: use configured transport
    Config.active_transport().ack(message, %{})
  end

  @doc """
  Manually negatively-acknowledges a message from within a handler.

  Use when you detect a permanent failure and want to route to DLQ
  immediately without going through the retry count.

      def handle(message, context) do
        if invalid_payload?(message.payload) do
          PhoenixMicro.Consumer.nack(message, context, :invalid_payload)
          :nack
        else
          process(message.payload)
          :ok
        end
      end
  """
  @spec nack(Message.t(), context(), term()) :: :ok
  def nack(message, context, reason \\ :nacked)

  def nack(%Message{} = message, %{transport_mod: transport_mod}, reason) do
    transport_mod.nack(message, reason, %{})
  end

  def nack(%Message{} = message, _context, reason) do
    Config.active_transport().nack(message, reason, %{})
  end

  # ---------------------------------------------------------------------------
  # Macros
  # ---------------------------------------------------------------------------

  defmacro __using__(_opts) do
    quote do
      @behaviour PhoenixMicro.Consumer

      import PhoenixMicro.Consumer,
        only: [
          topic: 1,
          concurrency: 1,
          retry: 1,
          middleware: 1,
          dead_letter_topic: 1,
          transport: 1,
          queue_group: 1,
          batch_size: 1,
          batch_timeout: 1,
          pipeline: 1
        ]

      Module.register_attribute(__MODULE__, :pm_topic, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_concurrency, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_retry_opts, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_middleware, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_dlq_topic, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_transport, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_queue_group, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_batch_size, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_batch_timeout, accumulate: false)
      Module.register_attribute(__MODULE__, :pm_pipeline, accumulate: false)

      @pm_concurrency 1
      @pm_retry_opts [max_attempts: 3, base_delay: 500, max_delay: 30_000, jitter: true]
      @pm_middleware []
      @pm_transport nil
      @pm_dlq_topic nil
      @pm_queue_group nil
      @pm_batch_size 1
      @pm_batch_timeout 1_000
      # :broadway (default) or :simple (legacy Task-based)
      @pm_pipeline :broadway

      @before_compile PhoenixMicro.Consumer
    end
  end

  defmacro topic(name) do
    quote do
      @pm_topic unquote(name)
    end
  end

  defmacro concurrency(n) do
    quote do
      @pm_concurrency unquote(n)
    end
  end

  defmacro retry(opts) do
    quote do
      @pm_retry_opts unquote(opts)
    end
  end

  defmacro middleware(mods) do
    quote do
      @pm_middleware unquote(mods)
    end
  end

  defmacro dead_letter_topic(name) do
    quote do
      @pm_dlq_topic unquote(name)
    end
  end

  defmacro transport(t) do
    quote do
      @pm_transport unquote(t)
    end
  end

  defmacro queue_group(g) do
    quote do
      @pm_queue_group unquote(g)
    end
  end

  @doc """
  Number of messages Broadway will accumulate before calling `handle_batch/4`.
  Set to `1` (default) to disable batching and process messages individually.
  """
  defmacro batch_size(n) do
    quote do
      @pm_batch_size unquote(n)
    end
  end

  @doc """
  Maximum time in milliseconds to wait before flushing an incomplete batch.
  Only relevant when `batch_size > 1`. Default: 1000ms.
  """
  defmacro batch_timeout(ms) do
    quote do
      @pm_batch_timeout unquote(ms)
    end
  end

  @doc """
  Processing mode. `:broadway` (default) uses a full Broadway pipeline with
  backpressure and demand-driven scheduling. `:simple` uses the legacy
  `Task.start` dispatch — useful when Broadway is not a dependency.
  """
  defmacro pipeline(mode) when mode in [:broadway, :simple] do
    quote do
      @pm_pipeline unquote(mode)
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      @doc false
      def __consumer_config__ do
        %{
          topic: @pm_topic,
          concurrency: @pm_concurrency,
          retry_opts: @pm_retry_opts,
          middleware: @pm_middleware,
          dlq_topic: @pm_dlq_topic,
          transport: @pm_transport,
          queue_group: @pm_queue_group,
          batch_size: @pm_batch_size,
          batch_timeout: @pm_batch_timeout,
          pipeline: @pm_pipeline
        }
      end

      # Default handle_error implementation — retry if attempts remain
      @impl PhoenixMicro.Consumer
      def handle_error(message, _error, _context) do
        {:retry, message}
      end

      defoverridable handle_error: 3
    end
  end

  # ---------------------------------------------------------------------------
  # Runtime: starts a consumer as a supervised worker
  # ---------------------------------------------------------------------------

  @doc """
  Starts a supervised consumer worker for the given consumer module.
  Called by `PhoenixMicro.Supervisor.ConsumerManager`.
  """
  @spec start_link(module(), keyword()) :: GenServer.on_start()
  def start_link(consumer_module, opts \\ []) do
    PhoenixMicro.Consumer.Worker.start_link(consumer_module, opts)
  end

  @doc """
  Returns the consumer configuration map for the given module.
  """
  @spec config(module()) :: map()
  def config(module), do: module.__consumer_config__()

  @doc """
  Invokes the consumer's handle/2 through its middleware chain.
  """
  @spec dispatch(module(), Message.t(), context()) :: :ok | {:error, term()}
  def dispatch(consumer_module, message, context) do
    config = consumer_module.__consumer_config__()
    middlewares = config.middleware

    handler = fn msg ->
      case consumer_module.handle(msg, context) do
        :ok -> :ok
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, reason}
        :nack -> {:error, :nacked}
        {:retry, _msg} -> {:error, :retry_requested}
      end
    end

    PhoenixMicro.Transport.build_chain(middlewares, handler).(message)
  end
end

defmodule PhoenixMicro.Consumer.Worker do
  @moduledoc """
  GenServer that manages the lifecycle of a single consumer subscription:
  subscribing to the transport, invoking the consumer module, and
  applying retry + DLQ logic.
  """

  use GenServer

  require Logger

  alias PhoenixMicro.{Config, Consumer, Message, Telemetry}
  alias PhoenixMicro.Consumer.RetryScheduler

  defstruct [:consumer_module, :config, :transport_mod, :subscription_ref]

  @spec start_link(module(), keyword()) :: GenServer.on_start()
  def start_link(consumer_module, opts \\ []) do
    GenServer.start_link(__MODULE__, {consumer_module, opts}, name: via_name(consumer_module))
  end

  @impl GenServer
  def init({consumer_module, _opts}) do
    consumer_cfg = Consumer.config(consumer_module)

    if not consumer_cfg.topic do
      raise "Consumer #{inspect(consumer_module)} must declare a topic via `topic \"...\"`"
    end

    transport_name = consumer_cfg.transport || Config.get(:transport, :memory)
    transport_mod = Config.transport_module(transport_name)

    state = %__MODULE__{
      consumer_module: consumer_module,
      config: consumer_cfg,
      transport_mod: transport_mod,
      subscription_ref: nil
    }

    send(self(), :subscribe)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:subscribe, state) do
    %{topic: topic, concurrency: concurrency, queue_group: group} = state.config
    worker = self()

    handler = fn message ->
      GenServer.call(worker, {:handle_message, message}, :infinity)
    end

    opts = [concurrency: concurrency]
    opts = if group, do: Keyword.put(opts, :queue_group, group), else: opts

    case state.transport_mod.subscribe(topic, handler, opts) do
      {:ok, ref} ->
        Logger.info("[Consumer] #{inspect(state.consumer_module)} subscribed to #{topic}")
        {:noreply, %{state | subscription_ref: ref}}

      {:error, reason} ->
        Logger.error(
          "[Consumer] Subscription failed for #{inspect(state.consumer_module)}: #{inspect(reason)}"
        )

        Process.send_after(self(), :subscribe, 2_000)
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call({:handle_message, message}, _from, state) do
    context = %{
      transport: state.config.transport || Config.get(:transport, :memory),
      topic: state.config.topic,
      attempt: message.attempt,
      transport_mod: state.transport_mod,
      message: message
    }

    start = System.monotonic_time()

    Telemetry.message_received(message.topic, %{
      consumer: state.consumer_module,
      attempt: message.attempt
    })

    result = Consumer.dispatch(state.consumer_module, message, context)

    case result do
      :ok ->
        duration = System.monotonic_time() - start

        Telemetry.message_processed(message.topic, %{
          consumer: state.consumer_module,
          duration: duration
        })

        state.transport_mod.ack(message, %{})
        {:reply, :ok, state}

      {:error, reason} ->
        handle_failure(message, reason, context, state)
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp handle_failure(message, reason, context, state) do
    retry_opts = Config.retry_opts(state.config.retry_opts)
    max_attempts = Keyword.get(retry_opts, :max_attempts, 3)

    {action, updated_message} =
      case state.consumer_module.handle_error(message, reason, context) do
        {:retry, msg} -> {:retry, msg}
        :nack -> {:nack, message}
        :ok -> {:ack, message}
      end

    cond do
      action == :ack ->
        state.transport_mod.ack(updated_message, %{})
        {:reply, :ok, state}

      action == :nack or updated_message.attempt >= max_attempts ->
        Logger.warning("[Consumer] Exhausted retries for #{message.id} on #{message.topic}")
        route_to_dlq(updated_message, reason, state)

        Telemetry.message_failed(message.topic, %{
          consumer: state.consumer_module,
          reason: reason,
          final: true
        })

        {:reply, {:error, reason}, state}

      true ->
        delay = RetryScheduler.next_delay(updated_message.attempt, retry_opts)

        Logger.info(
          "[Consumer] Retrying #{message.id} in #{delay}ms (attempt #{updated_message.attempt})"
        )

        Telemetry.message_failed(message.topic, %{
          consumer: state.consumer_module,
          reason: reason,
          attempt: updated_message.attempt
        })

        Process.send_after(
          self(),
          {:retry_message, Message.increment_attempt(updated_message)},
          delay
        )

        {:reply, {:error, :retrying}, state}
    end
  end

  defp route_to_dlq(message, reason, state) do
    dlq_topic = state.config.dlq_topic || "dlq.#{message.topic}"

    dlq_message =
      message
      |> Message.put_metadata(%{dlq_reason: inspect(reason), original_topic: message.topic})
      |> Map.put(:topic, dlq_topic)

    state.transport_mod.nack(dlq_message, reason, %{})
    Logger.warning("[Consumer] DLQ → #{dlq_topic} for message #{message.id}")
  end

  defp via_name(consumer_module) do
    {:via, Registry, {PhoenixMicro.Registry, {__MODULE__, consumer_module}}}
  end
end

defmodule PhoenixMicro.Consumer.RetryScheduler do
  @moduledoc """
  Computes the next retry delay using exponential backoff with optional jitter.

  Delegates to `PhoenixMicro.Utils.Backoff` for the core algorithm.
  """

  alias PhoenixMicro.Utils.Backoff

  @spec next_delay(pos_integer(), keyword()) :: non_neg_integer()
  def next_delay(attempt, opts) do
    base = Keyword.get(opts, :base_delay, 500)
    cap = Keyword.get(opts, :max_delay, 30_000)
    jitter = Keyword.get(opts, :jitter, true)

    Backoff.next_delay(attempt, base: base, cap: cap, jitter: jitter)
  end
end
