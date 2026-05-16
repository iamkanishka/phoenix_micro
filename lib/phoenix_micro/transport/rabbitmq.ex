defmodule PhoenixMicro.Transport.RabbitMQ do
  @moduledoc """
  RabbitMQ transport adapter using the `amqp` Hex package.

  `phoenix_micro` lists **no amqp dependency** — add it to YOUR app's `mix.exs`:

      {:amqp, "~> 3.3"}

  > #### rebar3 / Windows note {: .warning}
  >
  > The `amqp` package depends on `rabbit_common`, an Erlang library that builds
  > with **rebar3**. On Windows this requires `escript.exe` (part of the Erlang/OTP
  > installation) to be on your `PATH`. If you see a rebar3 compile error, either:
  >
  > - Ensure Erlang/OTP is installed and `escript.exe` is on PATH, or
  > - Use **NATS** or **Redis Streams** instead — both are pure Elixir with no
  >   rebar3 or native-code dependencies.

  ## Features

  - Persistent channels with automatic reconnection (exponential backoff).
  - Per-consumer `basic.qos` prefetch for backpressure control.
  - Dead-letter exchange (DLX) routing on NACK.
  - Topic exchange routing with `#` and `*` wildcards (AMQP convention).
  - Publisher confirms for reliable publishing.

  ## Configuration

      config :phoenix_micro,
        transports: [
          rabbitmq: [
            url: "amqp://guest:guest@localhost",
            exchange: "phoenix_micro",
            prefetch_count: 10,
            reconnect_interval: 2_000
          ]
        ]
  """

  use GenServer

  @behaviour PhoenixMicro.Transport

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}
  alias PhoenixMicro.Utils.Backoff

  @default_exchange "phoenix_micro"
  @dlx_exchange "phoenix_micro.dlx"

  defstruct [
    :conn,
    :channel,
    :config,
    :task_sup,
    # %{ref => %{queue: q, tag: tag, handler: fun}}
    :subscriptions,
    :reconnect_timer,
    reconnect_attempts: 0
  ]

  # ---------------------------------------------------------------------------
  # Public API
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
    config = Keyword.merge(Config.transport_config(:rabbitmq), opts)
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  # ---------------------------------------------------------------------------
  # Transport behaviour
  # ---------------------------------------------------------------------------

  # Private dispatcher — all AMQP calls go through here so the module
  # compiles without the :amqp dependency installed.
  defp amqp(mod, fun, args), do: apply(mod, fun, args)

  @impl PhoenixMicro.Transport
  def connect(config) do
    unless Code.ensure_loaded?(AMQP.Connection) do
      raise "PhoenixMicro RabbitMQ transport requires {:amqp, \"~> 3.3\"} " <>
              "in your app's mix.exs."
    end

    url = Keyword.get(config, :url, "amqp://localhost")

    case amqp(AMQP.Connection, :open, [url]) do
      {:ok, conn} ->
        {:ok, chan} = amqp(AMQP.Channel, :open, [conn])
        amqp(AMQP.Confirm, :select, [chan])

        exchange = Keyword.get(config, :exchange, @default_exchange)
        amqp(AMQP.Exchange, :topic, [chan, exchange, [durable: true]])
        amqp(AMQP.Exchange, :topic, [chan, @dlx_exchange, [durable: true]])

        prefetch = Keyword.get(config, :prefetch_count, 10)
        amqp(AMQP.Basic, :qos, [chan, [prefetch_count: prefetch]])

        state = %__MODULE__{
          conn: conn,
          channel: chan,
          config: config,
          subscriptions: %{}
        }

        {:ok, state}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl PhoenixMicro.Transport
  def publish(topic, %Message{} = message, opts) do
    GenServer.call(__MODULE__, {:publish, topic, message, opts})
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
  def ack(%Message{raw: %{tag: tag}}, _state) do
    GenServer.cast(__MODULE__, {:ack, tag})
  end

  def ack(_message, _transport_state), do: :ok

  @impl PhoenixMicro.Transport
  def nack(%Message{raw: %{tag: tag}}, reason, _state) do
    GenServer.cast(__MODULE__, {:nack, tag, reason})
  end

  def nack(_message, _reason, _transport_state), do: :ok

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
        Logger.info("[RabbitMQ] Connected")
        Telemetry.transport_connected(:rabbitmq)
        {:noreply, %{new_state | reconnect_attempts: 0}}

      {:error, reason} ->
        Logger.error("[RabbitMQ] Connection failed: #{inspect(reason)}")
        schedule_reconnect(state)
        {:noreply, %{state | reconnect_attempts: state.reconnect_attempts + 1}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.warning("[RabbitMQ] Connection lost: #{inspect(reason)}")
    Telemetry.transport_disconnected(:rabbitmq, reason)
    schedule_reconnect(state)
    {:noreply, %{state | conn: nil, channel: nil}}
  end

  @impl GenServer
  def handle_info({:basic_deliver, payload, meta}, state) do
    handle_delivery(payload, meta, state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:basic_cancel, _meta}, state) do
    Logger.warning("[RabbitMQ] Consumer cancelled by broker")
    schedule_reconnect(state)
    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:publish, _topic, _message, _opts}, _from, %{channel: nil} = state) do
    Logger.error("[RabbitMQ] Cannot publish — not connected")
    {:reply, {:error, :not_connected}, state}
  end

  @impl GenServer
  def handle_call({:publish, topic, %Message{} = message, opts}, _from, state) do
    exchange = Keyword.get(state.config, :exchange, @default_exchange)
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)
    payload = serializer.encode!(message.payload)

    amqp_opts = [
      content_type: serializer.content_type(),
      message_id: message.id,
      timestamp: DateTime.to_unix(message.timestamp),
      headers: encode_headers(message.headers),
      persistent: Keyword.get(opts, :persistent, true)
    ]

    case amqp(AMQP.Basic, :publish, [state.channel, exchange, topic, payload, amqp_opts]) do
      :ok ->
        amqp(AMQP.Confirm, :wait_for_confirms, [state.channel])
        Telemetry.message_published(topic, %{transport: :rabbitmq})
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:subscribe, topic, handler, opts}, _from, state) do
    exchange = Keyword.get(state.config, :exchange, @default_exchange)
    queue_name = Keyword.get(opts, :queue, topic_to_queue(topic))
    durable = Keyword.get(opts, :durable, true)

    {:ok, _queue} =
      amqp(AMQP.Queue, :declare, [
        state.channel,
        queue_name,
        [
          durable: durable,
          arguments: [
            {"x-dead-letter-exchange", :longstr, @dlx_exchange},
            {"x-dead-letter-routing-key", :longstr, "dlq.#{queue_name}"}
          ]
        ]
      ])

    amqp(AMQP.Queue, :bind, [state.channel, queue_name, exchange, [routing_key: topic]])

    {:ok, tag} = amqp(AMQP.Basic, :consume, [state.channel, queue_name])

    ref = make_ref()

    new_subs =
      Map.put(state.subscriptions, ref, %{
        queue: queue_name,
        tag: tag,
        handler: handler,
        opts: opts
      })

    {:reply, {:ok, ref}, %{state | subscriptions: new_subs}}
  end

  @impl GenServer
  def handle_call({:unsubscribe, ref}, _from, state) do
    case Map.get(state.subscriptions, ref) do
      %{tag: tag} ->
        amqp(AMQP.Basic, :cancel, [state.channel, tag])
        {:reply, :ok, %{state | subscriptions: Map.delete(state.subscriptions, ref)}}

      nil ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call(:disconnect, _from, state) do
    if state.channel, do: amqp(AMQP.Channel, :close, [state.channel])
    if state.conn, do: amqp(AMQP.Connection, :close, [state.conn])
    {:reply, :ok, %{state | conn: nil, channel: nil}}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    status = if state.conn && state.channel, do: :connected, else: :disconnected
    {:reply, status, state}
  end

  @impl GenServer
  def handle_cast({:ack, tag}, state) do
    if state.channel, do: amqp(AMQP.Basic, :ack, [state.channel, tag])
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:nack, tag, _reason}, state) do
    if state.channel, do: amqp(AMQP.Basic, :nack, [state.channel, tag, [requeue: false]])
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    if state.channel, do: amqp(AMQP.Channel, :close, [state.channel])
    if state.conn, do: amqp(AMQP.Connection, :close, [state.conn])
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp handle_delivery(raw_payload, meta, state) do
    serializer = Config.get(:serializer, PhoenixMicro.Serializer.JSON)

    Task.Supervisor.start_child(state.task_sup, fn ->
      start = System.monotonic_time()

      try do
        payload = serializer.decode!(raw_payload)
        routing_key = meta.routing_key
        attempt = extract_attempt(meta.headers)
        topic = routing_key
        Telemetry.message_received(topic, %{transport: :rabbitmq})

        message =
          Message.new(routing_key, payload,
            id: meta.message_id || Message.generate_id(),
            headers: decode_headers(meta.headers),
            attempt: attempt,
            raw: %{tag: meta.delivery_tag, meta: meta}
          )

        handler = find_handler(state.subscriptions, routing_key)

        case handler && handler.(message) do
          nil ->
            Logger.warning("[RabbitMQ] No handler for #{routing_key}")
            amqp(AMQP.Basic, :nack, [state.channel, meta.delivery_tag, [requeue: false]])

          :ok ->
            duration = System.monotonic_time() - start
            amqp(AMQP.Basic, :ack, [state.channel, meta.delivery_tag])
            Telemetry.message_processed(topic, %{transport: :rabbitmq, duration: duration})

          {:ok, _result} ->
            duration = System.monotonic_time() - start
            amqp(AMQP.Basic, :ack, [state.channel, meta.delivery_tag])
            Telemetry.message_processed(topic, %{transport: :rabbitmq, duration: duration})

          {:error, reason} ->
            amqp(AMQP.Basic, :nack, [state.channel, meta.delivery_tag, [requeue: false]])
            Telemetry.message_failed(topic, %{transport: :rabbitmq, reason: reason})

          :nack ->
            amqp(AMQP.Basic, :nack, [state.channel, meta.delivery_tag, [requeue: false]])
        end
      rescue
        e ->
          Logger.error("[RabbitMQ] Handler crash: #{Exception.message(e)}")
          amqp(AMQP.Basic, :nack, [state.channel, meta.delivery_tag, [requeue: false]])
      end
    end)
  end

  # Match delivery routing key against subscribed queue patterns
  defp find_handler(subscriptions, routing_key) do
    result =
      Enum.find(subscriptions, fn {_ref, %{queue: queue}} ->
        queue == routing_key or amqp_topic_matches?(queue, routing_key)
      end)

    case result do
      {_ref, %{handler: handler}} -> handler
      nil -> nil
    end
  end

  # AMQP topic exchange wildcard matching (* = one word, # = zero or more words)
  defp amqp_topic_matches?(pattern, topic) do
    p_parts = String.split(pattern, ".")
    t_parts = String.split(topic, ".")
    amqp_match(p_parts, t_parts)
  end

  defp amqp_match([], []), do: true
  defp amqp_match(["#"], _rest), do: true

  defp amqp_match(["#" | prest], [_tok | trest]) do
    amqp_match(["#" | prest], trest) or amqp_match(prest, trest)
  end

  defp amqp_match(["*" | prest], [_tok | trest]), do: amqp_match(prest, trest)
  defp amqp_match([same | prest], [same | trest]), do: amqp_match(prest, trest)
  defp amqp_match(_pat, _seg), do: false

  defp schedule_reconnect(state) do
    base = Keyword.get(state.config, :reconnect_interval, 2_000)

    interval =
      Backoff.next_delay(state.reconnect_attempts + 1, base: base, cap: 30_000, jitter: true)

    Process.send_after(self(), :connect, interval)

    attempts = state.reconnect_attempts + 1
    Logger.info("[RabbitMQ] Reconnecting in #{interval}ms (attempt #{attempts})")

    :ok
  end

  defp topic_to_queue(topic) do
    String.replace(topic, [".", "*", "#"], fn
      "." -> "."
      "*" -> "wildcard"
      "#" -> "multi"
    end)
  end

  defp encode_headers(headers) when is_map(headers) do
    Enum.map(headers, fn {k, v} -> {k, :longstr, v} end)
  end

  defp decode_headers(nil), do: %{}
  defp decode_headers(:undefined), do: %{}

  defp decode_headers(headers) when is_list(headers) do
    headers
    |> Enum.reject(fn {k, _type, _val} -> String.starts_with?(to_string(k), "x-death") end)
    |> Map.new(fn {k, _header_type, v} -> {to_string(k), to_string(v)} end)
  end

  defp extract_attempt(headers) do
    case decode_headers(headers) do
      %{"x-attempt" => attempt} -> String.to_integer(attempt)
      _other -> 1
    end
  end
end
