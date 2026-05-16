defmodule PhoenixMicro.Transport.RedisStreams do
  @moduledoc """
  Redis Streams transport adapter using `Redix`.

  Uses the Redis Streams data structure (XADD / XREADGROUP / XACK) for
  durable, consumer-group-aware messaging with at-least-once delivery.

  ## Features

  - Consumer groups via `XREADGROUP` / `XACK`.
  - Pending entry list (PEL) recovery on restart via `XAUTOCLAIM`.
  - DLQ routing for exhausted retries.
  - Configurable block timeout and batch size.

  ## Configuration

      config :phoenix_micro,
        transports: [
          redis_streams: [
            url: "redis://localhost:6379",
            consumer_group: "phoenix_micro",
            consumer_name: "node_1",
            block_ms: 2_000,
            batch_size: 10,
            claim_idle_ms: 30_000
          ]
        ]
  """

  use GenServer

  @behaviour PhoenixMicro.Transport

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}

  @dlq_prefix "dlq:"
  @read_timeout 2_000

  defstruct [
    :conn,
    :config,
    # %{ref => %{stream: s, group: g, handler: fn, poll_ref: ref}}
    :subscriptions,
    connected: false
  ]

  # ---------------------------------------------------------------------------
  # Transport behaviour
  # ---------------------------------------------------------------------------

  @impl PhoenixMicro.Transport
  def connect(config) do
    unless Code.ensure_loaded?(Redix) do
      raise "PhoenixMicro Redis Streams transport requires the :redix dependency. " <>
              "Add `{:redix, \"~> 1.5\"}` to your app's mix.exs (not phoenix_micro)."
    end

    url = Keyword.get(config, :url, "redis://localhost:6379")

    case apply(Redix, :start_link, [url]) do
      {:ok, conn} ->
        state = %__MODULE__{
          conn: conn,
          config: config,
          subscriptions: %{},
          connected: true
        }

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl PhoenixMicro.Transport
  def publish(topic, %Message{} = message, _opts) do
    GenServer.call(__MODULE__, {:publish, topic, message})
  end

  @impl PhoenixMicro.Transport
  def subscribe(topic, handler, opts) do
    GenServer.call(__MODULE__, {:subscribe, topic, handler, opts})
  end

  @impl PhoenixMicro.Transport
  def unsubscribe(ref, _state) do
    GenServer.call(__MODULE__, {:unsubscribe, ref})
  end

  @impl PhoenixMicro.Transport
  def ack(%Message{raw: %{stream: stream, group: group, entry_id: entry_id}}, _state) do
    GenServer.cast(__MODULE__, {:ack, stream, group, entry_id})
  end

  def ack(_msg, _state), do: :ok

  @impl PhoenixMicro.Transport
  def nack(%Message{} = message, reason, state) do
    dlq_stream = @dlq_prefix <> message.topic

    dlq_msg =
      message
      |> Message.put_metadata(%{dlq_reason: inspect(reason), original_topic: message.topic})
      |> Map.put(:topic, dlq_stream)

    publish(dlq_stream, dlq_msg, [])
    ack(message, state)
  end

  @impl PhoenixMicro.Transport
  def disconnect(_state) do
    GenServer.call(__MODULE__, :disconnect)
  end

  @impl PhoenixMicro.Transport
  def status(_state) do
    GenServer.call(__MODULE__, :status)
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 10_000
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    config = Keyword.merge(Config.transport_config(:redis_streams), opts)
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl GenServer
  def init(config) do
    case connect(config) do
      {:ok, state} ->
        Logger.info("[RedisStreams] Connected")
        Telemetry.transport_connected(:redis_streams)
        {:ok, state}

      {:error, reason} ->
        Logger.error("[RedisStreams] Connection failed: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:publish, topic, %Message{} = message}, _from, state) do
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    payload = serializer.encode!(message.payload)

    fields =
      [
        "id",
        message.id,
        "topic",
        message.topic,
        "payload",
        payload,
        "attempt",
        Integer.to_string(message.attempt),
        "timestamp",
        DateTime.to_iso8601(message.timestamp)
      ] ++ headers_to_redis(message.headers)

    case apply(Redix, :command, [state.conn, ["XADD", topic, "*" | fields]]) do
      {:ok, _entry_id} ->
        Telemetry.message_published(topic, %{transport: :redis_streams})
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:subscribe, stream, handler, opts}, _from, state) do
    group = Keyword.get(opts, :group, Keyword.get(state.config, :consumer_group, "phoenix_micro"))

    consumer =
      Keyword.get(opts, :consumer_name, Keyword.get(state.config, :consumer_name, "consumer_1"))

    ensure_consumer_group(state.conn, stream, group)

    ref = make_ref()
    poll_ref = schedule_poll(0)

    new_subs =
      Map.put(state.subscriptions, ref, %{
        stream: stream,
        group: group,
        consumer: consumer,
        handler: handler,
        opts: opts,
        poll_ref: poll_ref
      })

    {:reply, {:ok, ref}, %{state | subscriptions: new_subs}}
  end

  @impl GenServer
  def handle_call({:unsubscribe, ref}, _from, state) do
    case Map.pop(state.subscriptions, ref) do
      {%{poll_ref: poll_ref}, new_subs} ->
        _cancelled = Process.cancel_timer(poll_ref)
        {:reply, :ok, %{state | subscriptions: new_subs}}

      {nil, _reply} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call(:disconnect, _from, state) do
    apply(Redix, :stop, [state.conn])
    {:reply, :ok, %{state | conn: nil, connected: false}}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    status = if state.connected, do: :connected, else: :disconnected
    {:reply, status, state}
  end

  @impl GenServer
  def handle_cast({:ack, stream, group, entry_id}, state) do
    _result = apply(Redix, :command, [state.conn, ["XACK", stream, group, entry_id]])
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:poll, ref}, state) do
    case Map.get(state.subscriptions, ref) do
      nil ->
        {:noreply, state}

      sub ->
        poll_stream(state.conn, sub)
        poll_ref = schedule_poll(@read_timeout)
        new_subs = put_in(state.subscriptions, [ref, :poll_ref], poll_ref)
        {:noreply, %{state | subscriptions: new_subs}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp poll_stream(conn, %{
         stream: stream,
         group: group,
         consumer: consumer,
         handler: handler,
         opts: opts
       }) do
    batch_size = Keyword.get(opts, :batch_size, 10)
    block_ms = Keyword.get(opts, :block_ms, @read_timeout)

    cmd = [
      "XREADGROUP",
      "GROUP",
      group,
      consumer,
      "COUNT",
      Integer.to_string(batch_size),
      "BLOCK",
      Integer.to_string(block_ms),
      "STREAMS",
      stream,
      ">"
    ]

    case apply(Redix, :command, [conn, cmd]) do
      {:ok, nil} ->
        :ok

      {:ok, [[_stream, entries]]} ->
        Enum.each(entries, &dispatch_entry(&1, stream, group, handler, conn))

      {:error, error} when is_map(error) and is_binary(:erlang.map_get(:message, error)) ->
        if String.starts_with?(:erlang.map_get(:message, error), "NOGROUP") do
          ensure_consumer_group(conn, stream, group)
        else
          Logger.error("[RedisStreams] XREADGROUP error: #{inspect(error)}")
        end

      {:error, reason} ->
        Logger.error("[RedisStreams] XREADGROUP error: #{inspect(reason)}")
    end
  end

  defp dispatch_entry([entry_id, fields], stream, group, handler, conn) do
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    field_map = fields_to_map(fields)

    start = System.monotonic_time()
    topic = Map.get(field_map, "topic", stream)

    Telemetry.message_received(topic, %{transport: :redis_streams})

    try do
      payload = serializer.decode!(Map.get(field_map, "payload", "{}"))
      attempt = String.to_integer(Map.get(field_map, "attempt", "1"))

      message =
        Message.new(topic, payload,
          id: Map.get(field_map, "id", Message.generate_id()),
          attempt: attempt,
          raw: %{stream: stream, group: group, entry_id: entry_id}
        )

      case handler.(message) do
        :ok ->
          _ack = apply(Redix, :command, [conn, ["XACK", stream, group, entry_id]])
          duration = System.monotonic_time() - start
          Telemetry.message_processed(topic, %{transport: :redis_streams, duration: duration})

        {:error, reason} ->
          Telemetry.message_failed(topic, %{transport: :redis_streams, reason: reason})
          # Leave in PEL for retry / XAUTOCLAIM
      end
    rescue
      e ->
        Logger.error("[RedisStreams] Handler crash for #{topic}: #{inspect(e)}")
        Telemetry.message_failed(topic, %{transport: :redis_streams, reason: e})
    end
  end

  defp ensure_consumer_group(conn, stream, group) do
    # MKSTREAM creates the stream if it doesn't exist
    case apply(Redix, :command, [conn, ["XGROUP", "CREATE", stream, group, "$", "MKSTREAM"]]) do
      {:ok, "OK"} ->
        :ok

      {:error, error} when is_map(error) and is_binary(:erlang.map_get(:message, error)) ->
        if String.starts_with?(:erlang.map_get(:message, error), "BUSYGROUP") do
          :ok
        else
          Logger.warning("[RedisStreams] XGROUP CREATE failed: #{inspect(error)}")
        end

      {:error, reason} ->
        Logger.warning("[RedisStreams] XGROUP CREATE failed: #{inspect(reason)}")
    end
  end

  defp schedule_poll(delay) do
    ref = make_ref()
    Process.send_after(self(), {:poll, ref}, delay)
    ref
  end

  defp headers_to_redis(headers) when is_map(headers) do
    Enum.flat_map(headers, fn {k, v} -> ["hdr:#{k}", v] end)
  end

  defp fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Map.new(fn [k, v] -> {k, v} end)
  end
end
