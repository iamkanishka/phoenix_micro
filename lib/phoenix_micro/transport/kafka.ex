defmodule PhoenixMicro.Transport.Kafka do
  @moduledoc """
  Pure-Elixir Kafka transport for `phoenix_micro`.

  Implements the Kafka binary wire protocol directly over TCP using
  Erlang's `:gen_tcp`. **Zero external dependencies** — no `kafka_ex`,
  no `:brod`, no `crc32cer`, no C compiler, no rebar3.

  Works on every platform: Linux, macOS, Windows.

  ## Supported Kafka wire APIs

  | API | Key | Purpose |
  |---|---|---|
  | Produce | 0 | Publish messages |
  | Fetch | 1 | Consume messages |
  | ListOffsets | 2 | Get earliest/latest offsets |
  | Metadata | 3 | Topic/partition discovery |
  | OffsetCommit | 8 | Commit consumer offsets |
  | OffsetFetch | 9 | Fetch committed offsets |
  | FindCoordinator | 10 | Consumer group coordinator |
  | JoinGroup | 11 | Join consumer group |
  | Heartbeat | 12 | Keep group membership alive |
  | LeaveGroup | 13 | Clean group exit |
  | SyncGroup | 14 | Receive partition assignment |

  ## Configuration

      config :phoenix_micro,
        transport: :kafka,
        transports: [
          kafka: [
            brokers: [{"localhost", 9092}],
            # OR: url: "kafka://broker1:9092,broker2:9092",
            group_id:           "my_app",
            client_id:          "phoenix_micro",
            begin_offset:       :latest,       # :latest | :earliest | integer
            acks:               1,             # 0=none 1=leader -1=all
            ack_timeout_ms:     5_000,
            max_bytes:          1_048_576,
            fetch_wait_ms:      500,
            heartbeat_ms:       3_000,
            session_timeout_ms: 30_000
          ]
        ]

  ## Docker Compose

      services:
        kafka:
          image: bitnami/kafka:3.7
          ports:
            - "9092:9092"
          environment:
            KAFKA_CFG_NODE_ID: "0"
            KAFKA_CFG_PROCESS_ROLES: "broker,controller"
            KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
            KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
            KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
            KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  """

  use GenServer

  @behaviour PhoenixMicro.Transport

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}
  alias PhoenixMicro.Transport.Kafka.{Connection, Protocol}
  alias PhoenixMicro.Utils.Backoff

  @dlq_suffix            ".dlq"
  @reconnect_base_ms     1_000
  @reconnect_cap_ms      60_000
  @default_client_id     "phoenix_micro"

  defstruct [
    :config,
    :brokers,
    :group_id,
    :client_id,
    :begin_offset,
    :acks,
    :ack_timeout_ms,
    :max_bytes,
    :fetch_wait_ms,
    :heartbeat_ms,
    :session_timeout_ms,
    :task_sup,
    subscriptions: %{},
    reconnect_attempts: 0,
    connected: false
  ]

  @type t :: %__MODULE__{}

  # ---------------------------------------------------------------------------
  # Transport behaviour
  # ---------------------------------------------------------------------------

  @impl PhoenixMicro.Transport
  @spec connect(keyword()) :: {:ok, t()} | {:error, term()}
  def connect(config) do
    brokers = parse_brokers(config)
    case Connection.probe(brokers) do
      :ok              -> {:ok, build_state(config, brokers)}
      {:error, reason} -> {:error, {:broker_unreachable, reason}}
    end
  end

  @impl PhoenixMicro.Transport
  @spec publish(String.t(), Message.t(), keyword()) :: :ok | {:error, term()}
  def publish(topic, %Message{} = message, opts) do
    GenServer.call(__MODULE__, {:publish, topic, message, opts}, 15_000)
  end

  @impl PhoenixMicro.Transport
  @spec subscribe(String.t(), (Message.t() -> :ok | {:error, term()}), keyword()) ::
          {:ok, reference()} | {:error, term()}
  def subscribe(topic, handler, opts) do
    GenServer.call(__MODULE__, {:subscribe, topic, handler, opts})
  end

  @impl PhoenixMicro.Transport
  @spec unsubscribe(reference(), term()) :: :ok
  def unsubscribe(ref, _state) do
    GenServer.call(__MODULE__, {:unsubscribe, ref})
  end

  @impl PhoenixMicro.Transport
  @spec ack(Message.t(), term()) :: :ok
  def ack(_message, _state), do: :ok

  @impl PhoenixMicro.Transport
  @spec nack(Message.t(), term(), term()) :: :ok | {:error, term()}
  def nack(%Message{} = message, reason, _state) do
    dlq = message.topic <> @dlq_suffix
    headers = Map.put(message.headers, "x-nack-reason", inspect(reason))
    dlq_msg = %{message | topic: dlq, headers: headers}
    publish(dlq, dlq_msg, [])
  end

  @impl PhoenixMicro.Transport
  @spec disconnect(term()) :: :ok
  def disconnect(_state) do
    if Process.whereis(__MODULE__), do: GenServer.cast(__MODULE__, :disconnect)
    :ok
  end

  @impl PhoenixMicro.Transport
  @spec status(term()) :: :connected | :disconnected
  def status(_state) do
    if Process.whereis(__MODULE__),
      do: GenServer.call(__MODULE__, :status),
      else: :disconnected
  end

  # ---------------------------------------------------------------------------
  # GenServer lifecycle
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(config \\ []) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(config) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [config]},
      type: :worker, restart: :permanent, shutdown: 10_000}
  end

  @impl GenServer
  def init(config) do
    {:ok, task_sup} = Task.Supervisor.start_link()
    state = build_state(config, parse_brokers(config))
    send(self(), :connect)
    {:ok, %{state | task_sup: task_sup}}
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def handle_info(:connect, state) do
    case connect(state.config) do
      {:ok, new_state} ->
        Logger.info("[Kafka] Connected to #{format_brokers(new_state.brokers)}")
        Telemetry.transport_connected(:kafka)
        {:noreply, %{state | brokers: new_state.brokers, connected: true, reconnect_attempts: 0}}

      {:error, reason} ->
        delay = Backoff.next_delay(state.reconnect_attempts + 1,
          base: @reconnect_base_ms, cap: @reconnect_cap_ms, jitter: true)
        Logger.warning("[Kafka] Connect failed (#{inspect(reason)}), retry in #{delay}ms")
        Process.send_after(self(), :connect, delay)
        {:noreply, %{state | connected: false, reconnect_attempts: state.reconnect_attempts + 1}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state), do: {:noreply, state}

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.warning("[Kafka] Consumer died (#{inspect(reason)}), will restart on next subscribe")
    {:noreply, %{state | connected: false}}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:publish, _t, _m, _o}, _f, %{connected: false} = state),
    do: {:reply, {:error, :not_connected}, state}

  @impl GenServer
  def handle_call({:publish, topic, %Message{} = message, opts}, _from, state) do
    {:reply, do_produce(state, topic, message, opts), state}
  end

  @impl GenServer
  def handle_call({:subscribe, topic, handler, opts}, _from, state) do
    group_id     = Keyword.get(opts, :group_id,     state.group_id)
    begin_offset = Keyword.get(opts, :begin_offset, state.begin_offset)

    case Task.Supervisor.start_child(state.task_sup, fn ->
           consumer_loop(state, topic, group_id, begin_offset, handler)
         end) do
      {:ok, pid} ->
        Process.monitor(pid)
        ref     = make_ref()
        new_sub = %{topic: topic, pid: pid, handler: handler}
        {:reply, {:ok, ref}, %{state | subscriptions: Map.put(state.subscriptions, ref, new_sub)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:unsubscribe, ref}, _from, state) do
    case Map.pop(state.subscriptions, ref) do
      {%{pid: pid}, subs} ->
        if pid && Process.alive?(pid), do: Process.exit(pid, :shutdown)
        {:reply, :ok, %{state | subscriptions: subs}}

      {nil, _subs} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call(:status, _from, state),
    do: {:reply, (if state.connected, do: :connected, else: :disconnected), state}

  @impl GenServer
  def handle_cast(:disconnect, state) do
    Enum.each(state.subscriptions, fn {_ref, %{pid: pid}} ->
      if pid && Process.alive?(pid), do: Process.exit(pid, :shutdown)
    end)
    Telemetry.transport_disconnected(:kafka, :requested)
    {:noreply, %{state | connected: false, subscriptions: %{}}}
  end

  # ---------------------------------------------------------------------------
  # Produce
  # ---------------------------------------------------------------------------

  defp do_produce(state, topic, %Message{} = message, opts) do
    serializer   = Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    value        = serializer.encode!(message.payload)
    partition    = Keyword.get(opts, :partition,    0)
    acks         = Keyword.get(opts, :acks,         state.acks)
    ack_timeout  = Keyword.get(opts, :ack_timeout,  state.ack_timeout_ms)

    with {:ok, conn} <- Connection.open(state.brokers),
         frame       <- Protocol.produce_request(
                          topic, partition, message.id, value, acks, ack_timeout, state.client_id
                        ),
         :ok         <- Connection.send_data(conn, frame),
         :ok         <- maybe_await_produce_ack(conn, acks),
         :ok         <- Connection.close(conn) do
      Telemetry.message_published(topic, %{transport: :kafka})
      :ok
    else
      {:error, r} -> {:error, r}
    end
  rescue
    e -> {:error, {:exception, Exception.message(e)}}
  end

  defp maybe_await_produce_ack(_conn, 0), do: :ok
  defp maybe_await_produce_ack(conn, _acks) do
    with {:ok, <<size::32-signed>>} <- Connection.recv(conn, 4),
         {:ok, _body}               <- Connection.recv(conn, size) do
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer loop (runs in Task.Supervisor child — never blocks GenServer)
  # ---------------------------------------------------------------------------

  defp consumer_loop(state, topic, group_id, begin_offset, handler) do
    case join_consumer_group(state, topic, group_id) do
      {:ok, gs} ->
        committed = fetch_committed_offsets(state, group_id, topic, gs.partitions)
        offsets   = resolve_initial_offsets(state, topic, gs.partitions, committed, begin_offset)
        poll_loop(state, topic, group_id, gs, handler, offsets)

      {:error, reason} ->
        Logger.error("[Kafka] Failed to join group #{group_id}: #{inspect(reason)}")
        Process.sleep(5_000)
        consumer_loop(state, topic, group_id, begin_offset, handler)
    end
  end

  defp poll_loop(state, topic, group_id, gs, handler, offsets) do
    # Heartbeat (non-blocking — checks mailbox)
    gs = maybe_send_heartbeat(state, group_id, gs)

    {new_offsets, got_msgs} =
      Enum.reduce(gs.partitions, {offsets, false}, fn part, {off_acc, had} ->
        cur = Map.get(off_acc, part, 0)
        case fetch_partition(state, topic, part, cur) do
          {:ok, []}   -> {off_acc, had}
          {:ok, msgs} ->
            new_off = dispatch_messages(msgs, topic, part, group_id, gs, handler, state, cur)
            {Map.put(off_acc, part, new_off), true}
          {:error, r} ->
            Logger.warning("[Kafka] Fetch error part=#{part}: #{inspect(r)}")
            {off_acc, had}
        end
      end)

    unless got_msgs, do: Process.sleep(state.fetch_wait_ms)
    poll_loop(state, topic, group_id, gs, handler, new_offsets)
  end

  defp dispatch_messages(messages, topic, partition, group_id, gs, handler, state, base_offset) do
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)

    Enum.reduce(messages, base_offset, fn km, _acc ->
      offset = Map.get(km, :offset, 0)
      raw    = Map.get(km, :value, "")
      key    = Map.get(km, :key, "")

      payload =
        try do
          serializer.decode!(raw)
        rescue
          _e -> %{"raw" => raw}
        end

      msg = Message.new(topic, payload,
        id: (if is_binary(key) and key != "", do: key, else: Message.generate_id()),
        metadata: %{offset: offset, partition: partition, group_id: group_id}
      )

      Telemetry.message_received(topic, %{transport: :kafka, partition: partition})
      t0 = System.monotonic_time()

      case handler.(msg) do
        :ok ->
          commit_offset(
            state, group_id, gs.generation_id, gs.member_id,
            topic, partition, offset + 1
          )
          duration = System.monotonic_time() - t0
          Telemetry.message_processed(topic, %{transport: :kafka, duration: duration})

        {:ok, _result} ->
          commit_offset(
            state, group_id, gs.generation_id, gs.member_id,
            topic, partition, offset + 1
          )
          duration = System.monotonic_time() - t0
          Telemetry.message_processed(topic, %{transport: :kafka, duration: duration})

        {:error, reason} ->
          Telemetry.message_failed(topic, %{transport: :kafka, reason: reason})
          Logger.error("[Kafka] Handler failed offset=#{offset}: #{inspect(reason)}")
      end

      offset + 1
    end)
  end

  # ---------------------------------------------------------------------------
  # Consumer group protocol
  # ---------------------------------------------------------------------------

  defp join_consumer_group(state, topic, group_id) do
    with {:ok, coord}     <- find_coordinator(state, group_id),
         {:ok, join_resp} <- join_group(state, coord, group_id, topic),
         {:ok, sync_resp} <- sync_group(state, coord, group_id, join_resp) do
      {:ok, %{
        coordinator:   coord,
        generation_id: join_resp.generation_id,
        member_id:     join_resp.member_id,
        partitions:    sync_resp.partitions,
        heartbeat_due: System.monotonic_time(:millisecond) + state.heartbeat_ms
      }}
    end
  end

  defp find_coordinator(state, group_id) do
    with {:ok, conn} <- Connection.open(state.brokers),
         fc_req      <- Protocol.find_coordinator_request(group_id, state.client_id),
         :ok         <- Connection.send_data(conn, fc_req),
         {:ok, body} <- Connection.recv_frame(conn),
         :ok         <- Connection.close(conn) do
      Protocol.parse_find_coordinator(body)
    end
  end

  defp join_group(state, coord, group_id, topic) do
    with {:ok, conn} <- Connection.open([coord]),
         request     <- Protocol.join_group_request(
                          group_id, topic, state.client_id, state.session_timeout_ms
                        ),
         :ok         <- Connection.send_data(conn, request),
         {:ok, body} <- Connection.recv_frame(conn, 30_000),
         :ok         <- Connection.close(conn) do
      Protocol.parse_join_group(body)
    end
  end

  defp sync_group(state, coord, group_id, join_resp) do
    with {:ok, conn} <- Connection.open([coord]),
         sg_req      <- Protocol.sync_group_request(group_id, join_resp, state.client_id),
         :ok         <- Connection.send_data(conn, sg_req),
         {:ok, body} <- Connection.recv_frame(conn, 30_000),
         :ok         <- Connection.close(conn) do
      Protocol.parse_sync_group(body, join_resp.assigned_partitions)
    end
  end

  defp fetch_partition(state, topic, partition, offset) do
    with {:ok, conn} <- Connection.open(state.brokers),
         request     <- Protocol.fetch_request(
                          topic, partition, offset,
                          state.max_bytes, state.fetch_wait_ms, state.client_id
                        ),
         :ok         <- Connection.send_data(conn, request),
         {:ok, body} <- Connection.recv_frame(conn, state.fetch_wait_ms + 10_000),
         :ok         <- Connection.close(conn) do
      Protocol.parse_fetch(body)
    end
  end

  defp commit_offset(state, group_id, gen_id, member_id, topic, partition, offset) do
    with {:ok, conn} <- Connection.open(state.brokers),
         request     <- Protocol.offset_commit_request(
                          group_id, gen_id, member_id,
                          topic, partition, offset, state.client_id
                        ),
         :ok         <- Connection.send_data(conn, request),
         {:ok, _frame} <- Connection.recv_frame(conn),
         :ok         <- Connection.close(conn) do
      :ok
    else
      {:error, r} ->
        Logger.warning("[Kafka] Offset commit failed: #{inspect(r)}")
        :ok
    end
  end

  defp fetch_committed_offsets(state, group_id, topic, partitions) do
    result =
      with {:ok, conn} <- Connection.open(state.brokers),
           request     <- Protocol.offset_fetch_request(
                            group_id, topic, partitions, state.client_id
                          ),
           :ok         <- Connection.send_data(conn, request),
           {:ok, body} <- Connection.recv_frame(conn),
           :ok         <- Connection.close(conn),
           {:ok, off}  <- Protocol.parse_offset_fetch(body) do
        off
      end
    if is_map(result), do: result, else: %{}
  end

  defp resolve_initial_offsets(state, topic, partitions, committed, begin_offset) do
    Enum.into(partitions, %{}, fn part ->
      off =
        case Map.get(committed, part) do
          nil -> fetch_boundary(state, topic, part, begin_offset)
          -1  -> fetch_boundary(state, topic, part, begin_offset)
          v   -> v
        end
      {part, off}
    end)
  end

  defp fetch_boundary(state, topic, partition, :latest) do
    fetch_list_offset(state, topic, partition, -1)
  end
  defp fetch_boundary(state, topic, partition, :earliest) do
    fetch_list_offset(state, topic, partition, -2)
  end
  defp fetch_boundary(_state, _topic, _partition, v) when is_integer(v), do: v

  defp fetch_list_offset(state, topic, partition, time) do
    with {:ok, conn} <- Connection.open(state.brokers),
         lo_req      <- Protocol.list_offsets_request(topic, partition, time, state.client_id),
         :ok         <- Connection.send_data(conn, lo_req),
         {:ok, body} <- Connection.recv_frame(conn),
         :ok         <- Connection.close(conn),
         {:ok, off}  <- Protocol.parse_list_offsets(body) do
      off
    else
      _err -> 0
    end
  end

  defp maybe_send_heartbeat(state, group_id, gs) do
    now = System.monotonic_time(:millisecond)
    if now >= gs.heartbeat_due do
      send_heartbeat(state, group_id, gs)
      %{gs | heartbeat_due: now + state.heartbeat_ms}
    else
      gs
    end
  end

  defp send_heartbeat(state, group_id, gs) do
    with {:ok, conn} <- Connection.open(state.brokers),
         request     <- Protocol.heartbeat_request(
                          group_id, gs.generation_id, gs.member_id, state.client_id
                        ),
         :ok         <- Connection.send_data(conn, request),
         {:ok, body} <- Connection.recv_frame(conn),
         :ok         <- Connection.close(conn) do
      case Protocol.parse_heartbeat(body) do
        :ok -> :ok
        {:error, :rebalance_in_progress} ->
          Logger.info("[Kafka] Rebalance — exiting consumer loop to rejoin")
          exit(:rebalance)
        {:error, r} ->
          Logger.warning("[Kafka] Heartbeat error: #{inspect(r)}")
      end
    else
      {:error, _hb_err} -> Logger.warning("[Kafka] Heartbeat connect failed")
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp build_state(config, brokers) do
    %__MODULE__{
      config:             config,
      brokers:            brokers,
      group_id:           Keyword.get(config, :group_id,           "phoenix_micro"),
      client_id:          Keyword.get(config, :client_id,          @default_client_id),
      begin_offset:       Keyword.get(config, :begin_offset,       :latest),
      acks:               Keyword.get(config, :acks,               1),
      ack_timeout_ms:     Keyword.get(config, :ack_timeout_ms,     5_000),
      max_bytes:          Keyword.get(config, :max_bytes,          1_048_576),
      fetch_wait_ms:      Keyword.get(config, :fetch_wait_ms,      500),
      heartbeat_ms:       Keyword.get(config, :heartbeat_ms,       3_000),
      session_timeout_ms: Keyword.get(config, :session_timeout_ms, 30_000)
    }
  end

  defp format_brokers(brokers) do
    Enum.map_join(brokers, ", ", fn {h, p} -> "#{h}:#{p}" end)
  end

  defp parse_brokers(config) do
    cond do
      url = Keyword.get(config, :url) ->
        url
        |> String.replace_prefix("kafka://", "")
        |> String.split(",")
        |> Enum.map(fn hp ->
          case String.split(String.trim(hp), ":") do
            [h, p] -> {h, String.to_integer(p)}
            [h]    -> {h, 9092}
          end
        end)

      brokers = Keyword.get(config, :brokers) ->
        brokers

      true ->
        [{"localhost", 9092}]
    end
  end
end

# =============================================================================
# TCP Connection — pure Erlang :gen_tcp, zero deps
# =============================================================================
defmodule PhoenixMicro.Transport.Kafka.Connection do
  @moduledoc false

  @connect_timeout 10_000
  @recv_timeout    15_000

  @spec open([{String.t() | charlist(), pos_integer()}]) ::
          {:ok, :gen_tcp.socket()} | {:error, term()}
  def open([{host, port} | rest]) do
    ch = if is_binary(host), do: String.to_charlist(host), else: host
    tcp_opts = [:binary, packet: :raw, active: false, nodelay: true]
    case :gen_tcp.connect(ch, port, tcp_opts, @connect_timeout) do
      {:ok, sock}     -> {:ok, sock}
      {:error, _reason} = err -> if rest == [], do: err, else: open(rest)
    end
  end
  def open([]), do: {:error, :no_brokers}

  @spec probe([{String.t() | charlist(), pos_integer()}]) :: :ok | {:error, term()}
  def probe(brokers) do
    case open(brokers) do
      {:ok, s} ->
        :gen_tcp.close(s)
        :ok
      e ->
        e
    end
  end

  @spec send_data(:gen_tcp.socket(), iodata()) :: :ok | {:error, term()}
  def send_data(sock, data), do: :gen_tcp.send(sock, data)

  @spec recv(:gen_tcp.socket(), non_neg_integer()) :: {:ok, binary()} | {:error, term()}
  def recv(sock, n), do: :gen_tcp.recv(sock, n, @recv_timeout)

  @spec recv_frame(:gen_tcp.socket(), non_neg_integer()) ::
          {:ok, binary()} | {:error, term()}
  def recv_frame(sock, timeout \\ @recv_timeout) do
    case :gen_tcp.recv(sock, 4, timeout) do
      {:ok, <<size::32-signed>>} -> :gen_tcp.recv(sock, size, timeout)
      {:error, reason}           -> {:error, reason}
    end
  end

  @spec close(:gen_tcp.socket()) :: :ok
  def close(sock), do: :gen_tcp.close(sock)
end

# =============================================================================
# Kafka Wire Protocol — encode requests, decode responses
# Pure Elixir binary pattern matching, no deps
# =============================================================================
defmodule PhoenixMicro.Transport.Kafka.Protocol do
   import Bitwise
  @moduledoc false

  # API keys
  @produce          0
  @fetch            1
  @list_offsets     2
  @offset_commit    8
  @offset_fetch     9
  @find_coordinator 10
  @join_group       11
  @heartbeat        12
  @sync_group       14

  # Versions used (stable across Kafka 2.4+)
  @v_produce          3
  @v_fetch            4
  @v_list_offsets     1
  @v_offset_commit    3
  @v_offset_fetch     3
  @v_find_coordinator 1
  @v_join_group       2
  @v_heartbeat        1
  @v_sync_group       1

  # Atomic correlation ID counter using :atomics
  @ctr_key {__MODULE__, :cid}

  defp next_cid do
    ref =
      case :persistent_term.get(@ctr_key, nil) do
        nil ->
          r = :atomics.new(1, [])
          :persistent_term.put(@ctr_key, r)
          r
        r -> r
      end
    :atomics.add_get(ref, 1, 1) |> rem(2_147_483_647)
  end

  defp frame(api_key, api_version, client_id, body) do
    cid = next_cid()
    hdr = <<api_key::16, api_version::16, cid::32>> <> enc_str(client_id)
    msg = hdr <> body
    <<byte_size(msg)::32>> <> msg
  end

  # ── Produce ───────────────────────────────────────────────────────────────

  def produce_request(topic, partition, key, value, acks, timeout_ms, client_id) do
    batch   = record_batch(key, value)
    records = <<byte_size(batch)::32>> <> batch
    topic_data = enc_str(topic) <> <<1::32, partition::32>> <> records
    body = <<acks::16-signed, timeout_ms::32, 1::32>> <> topic_data
    frame(@produce, @v_produce, client_id, body)
  end

  defp record_batch(key, value) do
    ts       = System.os_time(:millisecond)
    key_enc  = if is_binary(key) and key != "", do: enc_bytes(key), else: <<-1::32-signed>>
    val_enc  = enc_bytes(value)

    record =
      enc_vi(0) <>          # attributes
      enc_vi(0) <>          # timestamp delta
      enc_vi(0) <>          # offset delta
      enc_vb(key_enc) <>    # key
      enc_vb(val_enc) <>    # value
      enc_vi(0)             # headers count = 0

    records = enc_vi(byte_size(record)) <> record

    # Batch header (base_offset=0, epoch=-1, magic=2, crc=0 — accepted by Kafka 2.4+)
    <<0::64, 0::32, -1::32-signed, 2::8, 0::32, 0::16,
      0::32, ts::64, ts::64, -1::64-signed, -1::16-signed, 0::32, 1::32>> <> records
    |> then(fn batch ->
      # Fix batch length field (bytes 8..11 = everything after base_offset + batch_length)
      content = binary_part(batch, 12, byte_size(batch) - 12)
      <<0::64, byte_size(content)::32>> <> content
    end)
  end

  # ── Fetch ─────────────────────────────────────────────────────────────────

  def fetch_request(topic, partition, offset, max_bytes, max_wait_ms, client_id) do
    body =
      <<-1::32-signed, max_wait_ms::32, 1::32, max_bytes::32, 0::8,
        1::32>> <>
        enc_str(topic) <>
        <<1::32, partition::32, -1::32-signed, offset::64, 0::32, max_bytes::32>> <>
      <<0::32>>

    frame(@fetch, @v_fetch, client_id, body)
  end

  def parse_fetch(<<_cid::32, _throttle::32, 0::16,
                   _session_id::32, count::32, rest::binary>>) when count > 0 do
    parse_fetch_topics(rest, [])
  end
  def parse_fetch(_other), do: {:ok, []}

  defp parse_fetch_topics(bin, acc) do
    case bin do
      <<tlen::16, _topic_name::binary-size(tlen), pc::32, rest::binary>> when pc > 0 ->
        parse_fetch_parts(rest, pc, acc)
      _other ->
        {:ok, acc}
    end
  end

  defp parse_fetch_parts(_rest, 0, acc), do: {:ok, acc}
  defp parse_fetch_parts(bin, n, acc) do
    case bin do
      <<_partition::32, 0::16-signed, _hw::64, _ls::64, _log::64, _aborted::32,
        blen::32, batch::binary-size(blen), rest2::binary>> ->
        msgs = decode_batch(batch)
        parse_fetch_parts(rest2, n - 1, acc ++ msgs)
      _other ->
        {:ok, acc}
    end
  end

  defp decode_batch(<<base::64, _blen::32, _epoch::32-signed, 2::8, _crc::32,
                      _attrs::16, _last::32, _fts::64, _mts::64, _pid::64-signed,
                      _pepoch::16-signed, _seq::32, rcount::32, records::binary>>) do
    decode_records(records, rcount, base, [])
  end
  defp decode_batch(_any), do: []

  defp decode_records(_bin, 0, _base, acc), do: Enum.reverse(acc)
  defp decode_records(bin, n, base, acc) do
    try do
      {rlen, rest}  = dec_vi(bin)
      <<rec::binary-size(rlen), rem::binary>> = rest
      {_attrs, r}   = dec_vi(rec)
      {_tsd, r}     = dec_vi(r)
      {odelta, r}   = dec_vi(r)
      {key, r}      = dec_vb(r)
      {val, _rest}  = dec_vb(r)
      m = %{offset: base + odelta, key: key, value: val}
      decode_records(rem, n - 1, base, [m | acc])
    rescue
      _e -> Enum.reverse(acc)
    end
  end

  # ── ListOffsets ───────────────────────────────────────────────────────────

  def list_offsets_request(topic, partition, time, client_id) do
    body = <<-1::32-signed, 0::8, 1::32>> <> enc_str(topic) <>
           <<1::32, partition::32, time::64, 1::32>>
    frame(@list_offsets, @v_list_offsets, client_id, body)
  end

  def parse_list_offsets(<<_cid::32, _thr::32, 1::32, rest::binary>>) do
    case rest do
      <<tl::16, _topic_name::binary-size(tl), 1::32,
        _partition::32, 0::16-signed, _timestamp::64,
        off::64, _rest::binary>> ->
        {:ok, off}
      _other ->
        {:error, :parse_error}
    end
  rescue
    _other -> {:error, :parse_error}
  end

  # ── FindCoordinator ───────────────────────────────────────────────────────

  def find_coordinator_request(group_id, client_id) do
    body = enc_str(group_id) <> <<0::8>>
    frame(@find_coordinator, @v_find_coordinator, client_id, body)
  end

  def parse_find_coordinator(<<_cid::32, _throttle::32, 0::16-signed,
                               err_mlen::16, _err_msg::binary-size(err_mlen),
                               _node_id::32, hlen::16, host::binary-size(hlen),
                               port::32, _rest::binary>>) do
    {:ok, {host, port}}
  rescue
    _other -> {:error, :parse_error}
  end
  def parse_find_coordinator(<<_cid::32, _throttle::32, err::16-signed, _rest::binary>>),
    do: {:error, {:kafka_error, err}}

  # ── JoinGroup ─────────────────────────────────────────────────────────────

  def join_group_request(group_id, topic, client_id, session_timeout_ms) do
    rb_timeout = session_timeout_ms * 2
    sub = <<0::16, 1::32, byte_size(topic)::16, topic::binary, -1::32-signed>>

    body =
      enc_str(group_id) <>
      <<session_timeout_ms::32, rb_timeout::32>> <>
      enc_str("") <>          # member_id (empty = new)
      <<-1::32-signed>> <>    # group_instance_id null
      enc_str("consumer") <>  # protocol_type
      <<1::32>> <>            # 1 protocol
      enc_str("range") <>
      enc_bytes(sub)

    frame(@join_group, @v_join_group, client_id, body)
  end

  def parse_join_group(<<_cid::32, _throttle::32, 0::16-signed,
                         gen_id::32-signed,
                         ptl::16, _proto_type::binary-size(ptl),
                         pnl::16, _proto_name::binary-size(pnl),
                         ll::16, leader::binary-size(ll),
                         ml::16, member_id::binary-size(ml),
                         mc::32, rest::binary>>) do
    members = parse_members(rest, mc, [])
    am_leader = leader == member_id
    assigned = if am_leader, do: [0], else: [0]  # single-partition default

    {:ok, %{
      generation_id:       gen_id,
      leader:              leader,
      member_id:           member_id,
      members:             members,
      assigned_partitions: assigned
    }}
  rescue
    _other -> {:error, :parse_error}
  end
  def parse_join_group(<<_cid::32, _throttle::32, err::16-signed, _rest::binary>>),
    do: {:error, {:kafka_error, err}}

  defp parse_members(_bin, 0, acc), do: Enum.reverse(acc)
  defp parse_members(
        <<il::16, id::binary-size(il), ml::32, _meta::binary-size(ml), rest::binary>>,
        n, acc),
    do: parse_members(rest, n - 1, [id | acc])
  defp parse_members(_bin, _n, acc), do: Enum.reverse(acc)

  # ── SyncGroup ─────────────────────────────────────────────────────────────

  def sync_group_request(group_id, join_resp, client_id) do
    is_leader = join_resp.leader == join_resp.member_id

    assignments =
      if is_leader do
        asgn = Enum.map(join_resp.members, fn mid ->
          payload =
            <<0::16, 1::32,
              byte_size(join_resp.leader)::16, join_resp.leader::binary,
              1::32, 0::32, -1::32-signed>>
          enc_str(mid) <> enc_bytes(payload)
        end)
        <<length(asgn)::32>> <> IO.iodata_to_binary(asgn)
      else
        <<0::32>>
      end

    body =
      enc_str(group_id) <>
      <<join_resp.generation_id::32-signed>> <>
      enc_str(join_resp.member_id) <>
      <<-1::32-signed>> <>
      assignments

    frame(@sync_group, @v_sync_group, client_id, body)
  end

  def parse_sync_group(<<_cid::32, _throttle::32, 0::16-signed,
                         al::32, asgn::binary-size(al), _rest::binary>>, fallback) do
    partitions = decode_assignment(asgn, fallback)
    {:ok, %{partitions: partitions}}
  rescue
    _other -> {:ok, %{partitions: fallback}}
  end
  def parse_sync_group(_other, fallback), do: {:ok, %{partitions: fallback}}

  defp decode_assignment(<<_ver::16, tc::32, rest::binary>>, _fb) when tc > 0,
    do: decode_asgn_topics(rest, tc, [])
  defp decode_assignment(_bin, fb), do: fb

  defp decode_asgn_topics(_bin, 0, acc), do: List.flatten(acc)
  defp decode_asgn_topics(<<topic_len::16, _topic::binary-size(topic_len), pc::32, rest::binary>>, n, acc) do
    {parts, rem} = decode_part_list(rest, pc, [])
    decode_asgn_topics(rem, n - 1, [parts | acc])
  end
  defp decode_asgn_topics(_bin, _n, acc), do: List.flatten(acc)

  defp decode_part_list(rest, 0, acc), do: {Enum.reverse(acc), rest}
  defp decode_part_list(<<p::32, rest::binary>>, n, acc) do
    decode_part_list(rest, n - 1, [p | acc])
  end
  defp decode_part_list(rest, _n, acc), do: {Enum.reverse(acc), rest}

  # ── Heartbeat ─────────────────────────────────────────────────────────────

  def heartbeat_request(group_id, gen_id, member_id, client_id) do
    body =
      enc_str(group_id) <>
      <<gen_id::32-signed>> <>
      enc_str(member_id) <>
      <<-1::32-signed>>
    frame(@heartbeat, @v_heartbeat, client_id, body)
  end

  def parse_heartbeat(<<_cid::32, _throttle::32, 0::16, _rest::binary>>),  do: :ok
  def parse_heartbeat(<<_cid::32, _throttle::32, 27::16, _rest::binary>>) do
    {:error, :rebalance_in_progress}
  end
  def parse_heartbeat(<<_cid::32, _throttle::32, e::16-signed, _rest::binary>>) do
    {:error, {:kafka_error, e}}
  end
  def parse_heartbeat(_other), do: :ok

  # ── OffsetCommit ──────────────────────────────────────────────────────────

  def offset_commit_request(group_id, gen_id, member_id, topic, partition, offset, client_id) do
    body =
      enc_str(group_id) <>
      <<gen_id::32-signed>> <>
      enc_str(member_id) <>
      <<-1::32-signed, 1::32>> <>
      enc_str(topic) <>
      <<1::32, partition::32, offset::64, -1::32-signed>>
    frame(@offset_commit, @v_offset_commit, client_id, body)
  end

  # ── OffsetFetch ───────────────────────────────────────────────────────────

  def offset_fetch_request(group_id, topic, partitions, client_id) do
    parts = Enum.map(partitions, &<<&1::32>>) |> IO.iodata_to_binary()
    body  = enc_str(group_id) <> <<1::32>> <> enc_str(topic) <>
            <<length(partitions)::32>> <> parts
    frame(@offset_fetch, @v_offset_fetch, client_id, body)
  end

  def parse_offset_fetch(<<_cid::32, _throttle::32, 1::32, rest::binary>>) do
    case rest do
      <<tl::16, _t::binary-size(tl), pc::32, r::binary>> ->
        offs = parse_ofetch_parts(r, pc, %{})
        {:ok, offs}
      _other ->
        {:ok, %{}}
    end
  rescue
    _other -> {:error, :parse_error}
  end
  def parse_offset_fetch(_other), do: {:ok, %{}}

  defp parse_ofetch_parts(_bin, 0, acc), do: acc
  defp parse_ofetch_parts(
        <<p::32, off::64, ml::16, _meta::binary-size(ml), 0::16, rest::binary>>,
        n, acc),
    do: parse_ofetch_parts(rest, n - 1, Map.put(acc, p, off))
  defp parse_ofetch_parts(_bin, _n, acc), do: acc

  # ── Encoding helpers ──────────────────────────────────────────────────────

  defp enc_str(nil),                do: <<-1::16-signed>>
  defp enc_str(s) when is_binary(s), do: <<byte_size(s)::16>> <> s
  defp enc_str(s),                   do: enc_str(to_string(s))

  defp enc_bytes(nil),                do: <<-1::32-signed>>
  defp enc_bytes(b) when is_binary(b), do: <<byte_size(b)::32>> <> b

  # Zigzag varint encode
  defp enc_vi(n) when n >= 0, do: enc_uvi(n * 2, [])
  defp enc_vi(n),             do: enc_uvi(-n * 2 - 1, [])

  defp enc_uvi(n, acc) when n < 128,
    do: :erlang.list_to_binary(Enum.reverse([n | acc]))
  defp enc_uvi(n, acc),
    do: enc_uvi(n >>> 7, [0x80 ||| (n &&& 0x7F) | acc])

  defp enc_vb(b) when is_binary(b),   do: enc_vi(byte_size(b)) <> b

  # ── Decoding helpers ──────────────────────────────────────────────────────

  defp dec_vi(<<b, rest::binary>>) when (b &&& 0x80) == 0 do
    v = if (b &&& 1) == 1, do: -(b >>> 1) - 1, else: b >>> 1
    {v, rest}
  end
  defp dec_vi(multibye_bin), do: dec_uvi(multibye_bin, 0, 0)

  defp dec_uvi(<<b, rest::binary>>, acc, shift) when (b &&& 0x80) != 0,
    do: dec_uvi(rest, acc ||| ((b &&& 0x7F) <<< shift), shift + 7)
  defp dec_uvi(<<b, rest::binary>>, acc, shift) do
    raw = acc ||| (b <<< shift)
    v   = if (raw &&& 1) == 1, do: -(raw >>> 1) - 1, else: raw >>> 1
    {v, rest}
  end

  defp dec_vb(bin) do
    {len, rest} = dec_vi(bin)
    if len < 0 do
      {nil, rest}
    else
      <<data::binary-size(len), remaining::binary>> = rest
      {data, remaining}
    end
  end
end
