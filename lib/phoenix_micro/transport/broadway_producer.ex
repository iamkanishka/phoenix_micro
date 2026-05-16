defmodule PhoenixMicro.Transport.BroadwayProducer do
  @moduledoc """
  A GenStage `Producer` that bridges any `PhoenixMicro.Transport` into a
  Broadway pipeline.

  This module implements the `Broadway.Producer` behaviour and sits at the
  head of every `PhoenixMicro.Pipeline`. It:

  1. Subscribes to the configured transport topic on `init`.
  2. Buffers incoming `PhoenixMicro.Message` structs in an internal queue.
  3. Drains the queue **on demand** — Broadway's processors pull messages,
     so the producer never pushes faster than downstream can handle (true
     backpressure).
  4. Emits Telemetry demand/buffer events for operational visibility.
  5. Re-subscribes automatically if the transport subscription is lost.

  ## How demand works

  Broadway calls `handle_demand/2` whenever a processor slot becomes free.
  `BroadwayProducer` dispatches up to `demand` buffered messages as
  `Broadway.Message` structs. If the buffer is empty it stores the pending
  demand and dispatches messages as they arrive via `handle_info/2`.

  This creates the classic GenStage pull model:

      Transport broker
            │  (push — unbounded)
            ▼
      BroadwayProducer buffer  ←── demand ── Broadway processors
            │  (pull — demand-driven)
            ▼
      Broadway processors

  The buffer size is capped by `:max_demand` (default 1000) to prevent
  unbounded memory growth. When the buffer is full, incoming messages are
  nacked so the broker can redeliver them later.
  """

  use GenStage

  require Logger

  alias PhoenixMicro.{Config, Message}

  @behaviour Broadway.Producer

  @default_max_demand 1_000
  @resubscribe_interval 2_000

  defstruct [
    :transport_mod,
    :subscription_ref,
    :topic,
    :opts,
    queue: :queue.new(),
    demand: 0,
    buffer_size: 0,
    max_demand: @default_max_demand
  ]

  # ---------------------------------------------------------------------------
  # Broadway.Producer callbacks
  # ---------------------------------------------------------------------------

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    # Called once before the pipeline supervisor starts.
    # We use it to inject our GenStage producer child spec.
    {[], broadway_opts}
  end

  @impl Broadway.Producer
  def prepare_for_draining(%__MODULE__{} = state) do
    # Called when Broadway is shutting down gracefully.
    # Stop accepting new messages from the transport.
    if state.subscription_ref do
      state.transport_mod.unsubscribe(state.subscription_ref, %{})
    end

    {:noreply, [], %{state | subscription_ref: nil}}
  end

  # ---------------------------------------------------------------------------
  # GenStage callbacks
  # ---------------------------------------------------------------------------

  @impl GenStage
  def init(opts) do
    topic = Keyword.fetch!(opts, :topic)
    transport_name = Keyword.get(opts, :transport, Config.get(:transport, :memory))
    transport_mod = Config.transport_module(transport_name)
    max_demand = Keyword.get(opts, :max_demand, @default_max_demand)

    state = %__MODULE__{
      transport_mod: transport_mod,
      topic: topic,
      opts: opts,
      max_demand: max_demand
    }

    # Subscribe asynchronously so init doesn't block
    send(self(), :subscribe)

    {:producer, state, dispatcher: GenStage.DemandDispatcher}
  end

  @impl GenStage
  def handle_demand(incoming_demand, state) do
    new_demand = state.demand + incoming_demand
    updated = %{state | demand: new_demand}

    :telemetry.execute(
      [:phoenix_micro, :pipeline, :demand],
      %{demand: incoming_demand, pending: new_demand, buffered: state.buffer_size},
      %{topic: state.topic}
    )

    dispatch(updated)
  end

  @impl GenStage
  def handle_info(:subscribe, state) do
    case subscribe_to_transport(state) do
      {:ok, _ref, new_state} ->
        Logger.info("[BroadwayProducer] Subscribed to #{state.topic}")
        {:noreply, [], new_state}

      {:error, reason} ->
        Logger.error(
          "[BroadwayProducer] Subscription failed for #{state.topic}: " <>
            "#{inspect(reason)}, retrying in #{@resubscribe_interval}ms"
        )

        Process.send_after(self(), :subscribe, @resubscribe_interval)
        {:noreply, [], state}
    end
  end

  # Incoming message from the transport (pushed to us via the handler closure)
  @impl GenStage
  def handle_info({:incoming, %Message{} = message}, state) do
    if state.buffer_size >= state.max_demand do
      # Buffer full — nack so the broker redelivers
      Logger.warning("[BroadwayProducer] Buffer full (#{state.max_demand}), nacking #{message.id}")

      state.transport_mod.nack(message, :buffer_full, %{})

      :telemetry.execute(
        [:phoenix_micro, :pipeline, :buffer_full],
        %{count: 1},
        %{topic: state.topic, max_demand: state.max_demand}
      )

      {:noreply, [], state}
    else
      new_queue = :queue.in(message, state.queue)
      new_state = %{state | queue: new_queue, buffer_size: state.buffer_size + 1}

      :telemetry.execute(
        [:phoenix_micro, :pipeline, :enqueued],
        %{buffer_size: new_state.buffer_size},
        %{topic: state.topic}
      )

      dispatch(new_state)
    end
  end

  @impl GenStage
  def handle_info({:resubscribe}, state) do
    send(self(), :subscribe)
    {:noreply, [], %{state | subscription_ref: nil}}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp subscribe_to_transport(state) do
    producer_pid = self()

    handler = fn %Message{} = msg ->
      send(producer_pid, {:incoming, msg})
      :ok
    end

    sub_opts = [
      # Transport delivers to us; we control concurrency
      concurrency: 1,
      queue_group: Keyword.get(state.opts, :queue_group)
    ]

    case state.transport_mod.subscribe(state.topic, handler, sub_opts) do
      {:ok, ref} ->
        {:ok, ref, %{state | subscription_ref: ref}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Dispatch up to `state.demand` messages from the queue as Broadway.Message structs
  defp dispatch(%__MODULE__{demand: 0} = state), do: {:noreply, [], state}
  defp dispatch(%__MODULE__{buffer_size: 0} = state), do: {:noreply, [], state}

  defp dispatch(state) do
    {broadway_messages, new_state} = take_from_queue(state, state.demand, [])
    {:noreply, broadway_messages, new_state}
  end

  defp take_from_queue(state, 0, acc) do
    {Enum.reverse(acc), state}
  end

  defp take_from_queue(%{buffer_size: 0} = state, _demand, acc) do
    {Enum.reverse(acc), state}
  end

  defp take_from_queue(state, remaining, acc) do
    case :queue.out(state.queue) do
      {{:value, message}, new_queue} ->
        broadway_msg = wrap_broadway_message(message, state)

        new_state = %{
          state
          | queue: new_queue,
            buffer_size: state.buffer_size - 1,
            demand: state.demand - 1
        }

        take_from_queue(new_state, remaining - 1, [broadway_msg | acc])

      {:empty, _queue} ->
        {Enum.reverse(acc), %{state | demand: remaining}}
    end
  end

  defp wrap_broadway_message(%Message{} = message, state) do
    %Broadway.Message{
      data: message,
      acknowledger: {__MODULE__, {state.transport_mod, message}, %{}}
    }
  end

  # ---------------------------------------------------------------------------
  # Broadway acknowledger callbacks
  # ---------------------------------------------------------------------------

  @doc false
  def ack({transport_mod, _message}, successful, failed) do
    Enum.each(successful, fn %Broadway.Message{data: msg} ->
      transport_mod.ack(msg, %{})
    end)

    Enum.each(failed, fn %Broadway.Message{data: msg, status: status} ->
      reason = extract_failure_reason(status)
      transport_mod.nack(msg, reason, %{})
    end)
  end

  defp extract_failure_reason({:failed, reason}), do: reason
  defp extract_failure_reason({:throw, value, _stacktrace}), do: {:throw, value}
  defp extract_failure_reason({:error, reason, _stacktrace}), do: {:error, reason}
  defp extract_failure_reason(other), do: other
end
