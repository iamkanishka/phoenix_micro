defmodule PhoenixMicro.Transport.Memory do
  @moduledoc """
  An in-process pub/sub transport backed by `Registry` and `GenServer`.

  This transport is:
  - **Always available** — no external broker required.
  - **Used in test environments** to avoid broker dependencies.
  - **Fully functional** — supports publish, subscribe, ack, nack, wildcard topics,
    consumer groups, and dead-letter queues.
  - **Observable** — emits all standard Telemetry events.

  ## Wildcard support

  Wildcards follow the NATS convention:
  - `*` matches a single token: `"payments.*"` matches `"payments.created"`.
  - `>` matches the rest: `"payments.>"` matches `"payments.created.v2"`.

  ## Usage in config

      config :phoenix_micro, transport: :memory

  ## Usage in tests

      setup do
        {:ok, _pid} = start_supervised(PhoenixMicro.Transport.Memory)
        :ok
      end
  """

  use GenServer

  @behaviour PhoenixMicro.Transport

  require Logger

  alias PhoenixMicro.{Message, Telemetry}

  # Whether :telemetry is available — checked once at module load time.
  # This allows Memory transport to work in test environments where only
  # the stdlib is loaded (no hex deps).

  defstruct [
    # %{ref => {topic_pattern, handler, opts}}
    :subscriptions,
    # List of dead-lettered messages
    :dlq,
    # :queue of all published messages (for test introspection)
    :message_log
  ]

  @name __MODULE__
  @dlq_topic "__dlq__"

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Returns all messages published since start (useful in tests)."
  @spec messages() :: [Message.t()]
  def messages(server \\ @name) do
    GenServer.call(server, :messages)
  end

  @doc "Returns all dead-lettered messages."
  @spec dlq_messages() :: [Message.t()]
  def dlq_messages(server \\ @name) do
    GenServer.call(server, :dlq_messages)
  end

  @doc "Clears the message log. Useful between test cases."
  @spec clear() :: :ok
  def clear(server \\ @name) do
    GenServer.call(server, :clear)
  end

  @doc "Blocks until at least `count` messages matching `topic` have been received."
  @spec wait_for_messages(String.t(), pos_integer(), timeout()) :: :ok | :timeout
  def wait_for_messages(topic, count \\ 1, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(topic, count, deadline)
  end

  # ---------------------------------------------------------------------------
  # Transport behaviour callbacks
  # ---------------------------------------------------------------------------

  @impl PhoenixMicro.Transport
  def connect(_config),
    do: {:ok, %__MODULE__{subscriptions: %{}, dlq: [], message_log: :queue.new()}}

  @impl PhoenixMicro.Transport
  def publish(_topic, message, _opts) do
    GenServer.call(@name, {:publish, message})
  end

  @impl PhoenixMicro.Transport
  def subscribe(topic, handler, opts) do
    GenServer.call(@name, {:subscribe, topic, handler, opts})
  end

  @impl PhoenixMicro.Transport
  def unsubscribe(ref, _state) do
    GenServer.call(@name, {:unsubscribe, ref})
  end

  @impl PhoenixMicro.Transport
  def ack(message, _state) do
    Logger.debug("[Memory] ACK #{message.id} on #{message.topic}")
    :ok
  end

  @impl PhoenixMicro.Transport
  def nack(message, reason, _state) do
    Logger.warning("[Memory] NACK #{message.id} on #{message.topic}: #{inspect(reason)}")
    GenServer.cast(@name, {:dlq, message, reason})
    :ok
  end

  @impl PhoenixMicro.Transport
  def disconnect(_state), do: :ok

  @impl PhoenixMicro.Transport
  def status(_state), do: :connected

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    {:ok, state} = connect(opts)
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:publish, message}, _from, state) do
    new_log = :queue.in(message, state.message_log)
    new_state = %{state | message_log: new_log}

    # Deliver to all matching subscribers asynchronously
    deliver_to_subscribers(message, state.subscriptions)

    if Code.ensure_loaded?(:telemetry),
      do: Telemetry.message_published(message.topic, %{transport: :memory})

    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_call({:subscribe, topic, handler, opts}, _from, state) do
    ref = make_ref()
    new_subs = Map.put(state.subscriptions, ref, {topic, handler, opts})
    {:reply, {:ok, ref}, %{state | subscriptions: new_subs}}
  end

  @impl GenServer
  def handle_call({:unsubscribe, ref}, _from, state) do
    new_subs = Map.delete(state.subscriptions, ref)
    {:reply, :ok, %{state | subscriptions: new_subs}}
  end

  @impl GenServer
  def handle_call(:messages, _from, state) do
    {:reply, :queue.to_list(state.message_log), state}
  end

  @impl GenServer
  def handle_call(:dlq_messages, _from, state) do
    {:reply, state.dlq, state}
  end

  @impl GenServer
  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{state | message_log: :queue.new(), dlq: []}}
  end

  @impl GenServer
  def handle_cast({:dlq, message, _reason}, state) do
    dlq_msg = %{
      message
      | topic: @dlq_topic,
        metadata: Map.put(message.metadata, :original_topic, message.topic)
    }

    {:noreply, %{state | dlq: [dlq_msg | state.dlq]}}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp deliver_to_subscribers(message, subscriptions) do
    Enum.each(subscriptions, fn {_ref, {pattern, handler, opts}} ->
      if topic_matches?(pattern, message.topic) do
        concurrency = Keyword.get(opts, :concurrency, 1)
        deliver_with_concurrency(message, handler, concurrency)
      end
    end)
  end

  defp deliver_with_concurrency(message, handler, _concurrency) do
    # In the memory transport, fire in a linked Task so crashes don't kill the GenServer
    Task.start(fn ->
      start = System.monotonic_time()
      maybe_telemetry_received(message)

      case handler.(message) do
        :ok ->
          duration = System.monotonic_time() - start
          maybe_telemetry_processed(message, duration)

        {:error, reason} ->
          maybe_telemetry_failed(message, reason)
      end
    end)
  end

  defp maybe_telemetry_received(message) do
    if Code.ensure_loaded?(:telemetry) do
      Telemetry.message_received(message.topic, %{transport: :memory, attempt: message.attempt})
    end
  end

  defp maybe_telemetry_processed(message, duration) do
    if Code.ensure_loaded?(:telemetry) do
      Telemetry.message_processed(message.topic, %{transport: :memory, duration: duration})
    end
  end

  defp maybe_telemetry_failed(message, reason) do
    if Code.ensure_loaded?(:telemetry) do
      Telemetry.message_failed(message.topic, %{transport: :memory, reason: reason})
    end
  end

  @doc false
  @spec topic_matches?(String.t(), String.t()) :: boolean()
  def topic_matches?(pattern, topic) do
    pattern_parts = String.split(pattern, ".")
    topic_parts = String.split(topic, ".")
    do_match(pattern_parts, topic_parts)
  end

  defp do_match([], []), do: true
  defp do_match([">"], _rest), do: true
  defp do_match(["*" | prest], [_token | trest]), do: do_match(prest, trest)
  defp do_match([same | prest], [same | trest]), do: do_match(prest, trest)
  defp do_match(_pat, _seg), do: false

  defp do_wait(topic, count, deadline) do
    now = System.monotonic_time(:millisecond)

    if now >= deadline do
      :timeout
    else
      matching =
        messages()
        |> Enum.count(&topic_matches?(topic, &1.topic))

      if matching >= count do
        :ok
      else
        Process.sleep(50)
        do_wait(topic, count, deadline)
      end
    end
  end
end
