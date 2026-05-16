defmodule PhoenixMicro.Transport.NATS do
  @moduledoc """
  NATS transport adapter using `Gnat`.

  `phoenix_micro` lists **no gnat dependency** — add it to YOUR app's `mix.exs`:

      {:gnat, "~> 1.7"}

  Gnat is pure Elixir — no rebar3, no C compiler, works on all platforms.

  ## Features

  - Core NATS pub/sub with native wildcard support (`*`, `>`).
  - Queue groups for load-balanced consumer groups.
  - Request/reply for RPC patterns.
  - Automatic reconnect with exponential backoff on connection loss.
  - Task.Supervisor-based dispatch (crashes don't kill the transport).
  - Full telemetry coverage.

  ## Configuration

      config :phoenix_micro,
        transport: :nats,
        transports: [
          nats: [
            host: "localhost",
            port: 4222,
            queue_group: "my_app",
            username: "user",    # optional
            password: "pass",    # optional
            tls: false           # optional
          ]
        ]
  """

  use GenServer

  @behaviour PhoenixMicro.Transport

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}
  alias PhoenixMicro.Utils.Backoff

  @reconnect_base_ms 1_000
  @reconnect_cap_ms 60_000
  @default_rpc_timeout 5_000

  defstruct [
    :conn,
    :config,
    :task_sup,
    # %{ref => %{sid: sid, handler: fun, topic: pattern}}
    subscriptions: %{},
    reconnect_attempts: 0,
    connected: false
  ]

  # ---------------------------------------------------------------------------
  # Public API / Transport behaviour
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    config = Keyword.merge(Config.transport_config(:nats), opts)
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @impl PhoenixMicro.Transport
  @spec connect(keyword()) :: {:ok, struct()} | {:error, term()}
  def connect(config) do
    unless Code.ensure_loaded?(Gnat) do
      raise "PhoenixMicro NATS transport requires {:gnat, \"~> 1.7\"} in your app's mix.exs"
    end

    host = Keyword.get(config, :host, "localhost")
    port = Keyword.get(config, :port, 4222)

    settings =
      %{host: host, port: port}
      |> maybe_put(:username, Keyword.get(config, :username))
      |> maybe_put(:password, Keyword.get(config, :password))
      |> maybe_put(:tls, if(Keyword.get(config, :tls, false), do: [], else: nil))

    case apply(Gnat, :start_link, [settings]) do
      {:ok, conn} ->
        {:ok, %__MODULE__{conn: conn, config: config, subscriptions: %{}, connected: true}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl PhoenixMicro.Transport
  def publish(topic, %Message{} = message, opts) do
    timeout = Keyword.get(opts, :timeout, @default_rpc_timeout)
    GenServer.call(__MODULE__, {:publish, topic, message, opts}, timeout)
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
  # NATS Core is fire-and-forget; ack is implicit
  def ack(_message, _state), do: :ok

  @impl PhoenixMicro.Transport
  # NATS Core has no nack concept; publish to DLQ instead
  def nack(%Message{topic: topic} = message, _reason, _state) do
    dlq_topic = topic <> ".dlq"
    publish(dlq_topic, message, [])
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
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)
    {:ok, task_sup} = Task.Supervisor.start_link()
    send(self(), :connect)
    {:ok, %__MODULE__{config: config, subscriptions: %{}, task_sup: task_sup}}
  end

  @impl GenServer
  def handle_info(:connect, state) do
    case connect(state.config) do
      {:ok, new_state} ->
        Process.monitor(new_state.conn)
        Logger.info("[NATS] Connected to #{state.config[:host]}:#{state.config[:port]}")
        Telemetry.transport_connected(:nats)

        {:noreply,
         %{state | conn: new_state.conn, connected: true, reconnect_attempts: 0}}

      {:error, reason} ->
        interval = Backoff.next_delay(state.reconnect_attempts + 1,
          base: @reconnect_base_ms, cap: @reconnect_cap_ms, jitter: true)
        Logger.warning("[NATS] Connection failed (#{inspect(reason)}), retrying in #{interval}ms")
        Process.send_after(self(), :connect, interval)
        {:noreply, %{state | connected: false, reconnect_attempts: state.reconnect_attempts + 1}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, %{conn: pid} = state) do
    Logger.warning("[NATS] Connection lost: #{inspect(reason)}")
    Telemetry.transport_disconnected(:nats, reason)
    interval = Backoff.next_delay(1,
      base: @reconnect_base_ms, cap: @reconnect_cap_ms, jitter: true)
    Process.send_after(self(), :connect, interval)
    {:noreply, %{state | conn: nil, connected: false, subscriptions: %{}}}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # Some other monitored process — ignore
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:msg, %{topic: topic, body: body} = msg}, state) do
    headers = Map.get(msg, :headers)
    reply_to = Map.get(msg, :reply_to)
    handler = find_handler(state.subscriptions, topic)

    if handler do
      dispatch(state.task_sup, topic, body, headers, reply_to, handler)
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:publish, _topic, _msg, _opts}, _from, %{connected: false} = state) do
    {:reply, {:error, :not_connected}, state}
  end

  @impl GenServer
  def handle_call({:publish, topic, %Message{} = message, opts}, _from, state) do
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    payload = serializer.encode!(message.payload)

    headers =
      [{"x-message-id", message.id}, {"x-attempt", Integer.to_string(message.attempt)}] ++
        Enum.map(message.headers, fn {k, v} -> {k, to_string(v)} end)

    pub_opts =
      if message.reply_to,
        do: [reply_to: message.reply_to, headers: headers],
        else: [headers: headers]

    if Keyword.get(opts, :sync, false) do
      timeout = Keyword.get(opts, :timeout, @default_rpc_timeout)
      case apply(Gnat, :request, [state.conn, topic, payload, [receive_timeout: timeout]]) do
        {:ok, _reply} ->
          Telemetry.message_published(topic, %{transport: :nats})
          {:reply, :ok, state}
        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      case apply(Gnat, :pub, [state.conn, topic, payload, pub_opts]) do
        :ok ->
          Telemetry.message_published(topic, %{transport: :nats})
          {:reply, :ok, state}
        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl GenServer
  def handle_call({:subscribe, topic, handler, opts}, _from, state) do
    queue_group = Keyword.get(opts, :queue_group, Keyword.get(state.config, :queue_group))
    sub_opts = if queue_group, do: [queue_group: queue_group], else: []

    case apply(Gnat, :sub, [state.conn, self(), topic, sub_opts]) do
      {:ok, sid} ->
        ref = make_ref()
        subs = Map.put(state.subscriptions, ref, %{sid: sid, handler: handler, topic: topic})
        {:reply, {:ok, ref}, %{state | subscriptions: subs}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:unsubscribe, ref}, _from, state) do
    case Map.pop(state.subscriptions, ref) do
      {%{sid: sid}, new_subs} ->
        apply(Gnat, :unsub, [state.conn, sid])
        {:reply, :ok, %{state | subscriptions: new_subs}}

      {nil, _subs} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call(:disconnect, _from, state) do
    if state.conn, do: apply(Gnat, :stop, [state.conn])
    {:reply, :ok, %{state | conn: nil, connected: false}}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    {:reply, if(state.connected, do: :connected, else: :disconnected), state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.conn, do: catch_exit(apply(Gnat, :stop, [state.conn]))
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp dispatch(task_sup, topic, body, raw_headers, reply_to, handler) do
    Task.Supervisor.start_child(task_sup, fn ->
      start = System.monotonic_time()
      Telemetry.message_received(topic, %{transport: :nats})
      serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)

      try do
        headers = parse_headers(raw_headers)
        payload = serializer.decode!(body)

        message =
          Message.new(topic, payload,
            id: Map.get(headers, "x-message-id", Message.generate_id()),
            headers: headers,
            reply_to: reply_to,
            attempt: parse_attempt(headers)
          )

        case handler.(message) do
          :ok ->
            duration = System.monotonic_time() - start
            Telemetry.message_processed(topic, %{transport: :nats, duration: duration})

          {:ok, _result} ->
            duration = System.monotonic_time() - start
            Telemetry.message_processed(topic, %{transport: :nats, duration: duration})

          {:error, reason} ->
            Telemetry.message_failed(topic, %{transport: :nats, reason: reason})
        end
      rescue
        e ->
          Logger.error("[NATS] Handler crash on #{topic}: #{Exception.message(e)}")
          Telemetry.message_failed(topic, %{transport: :nats, reason: e})
      end
    end)
  end

  defp find_handler(subscriptions, topic) do
    result =
      Enum.find(subscriptions, fn {_ref, %{topic: pattern}} ->
        topic_matches?(pattern, topic)
      end)

    case result do
      {_ref, %{handler: handler}} -> handler
      nil -> nil
    end
  end

  # NATS wildcard matching — inlined to avoid dependency on Memory transport
  defp topic_matches?(pattern, topic) do
    p_parts = String.split(pattern, ".")
    t_parts = String.split(topic, ".")
    do_match(p_parts, t_parts)
  end

  defp do_match([], []), do: true
  defp do_match([">"], _rest), do: true
  defp do_match(["*" | prest], [_token | trest]), do: do_match(prest, trest)
  defp do_match([same | prest], [same | trest]), do: do_match(prest, trest)
  defp do_match(_pat, _seg), do: false

  defp parse_headers(nil), do: %{}
  defp parse_headers(headers) when is_list(headers) do
    Map.new(headers, fn {k, v} -> {to_string(k), to_string(v)} end)
  end
  defp parse_headers(headers) when is_map(headers), do: headers

  defp parse_attempt(headers) do
    case Map.get(headers, "x-attempt") do
      nil -> 1
      v -> String.to_integer(v)
    end
  rescue
    _e -> 1
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, val), do: Map.put(map, key, val)

  defp catch_exit(result), do: result
end
