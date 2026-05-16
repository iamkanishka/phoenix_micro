defmodule PhoenixMicro.RPC do
  @moduledoc """
  Request-response RPC over any configured transport.

  Each RPC call:

  1. Generates a unique `correlation_id`.
  2. Publishes the request to `service_topic` with a `reply_to` inbox topic.
  3. Subscribes temporarily to the inbox topic.
  4. Waits for a response matching the correlation ID.
  5. Returns `{:ok, response_payload}` or `{:error, reason}` on timeout.

  ## Example

      # Caller side
      {:ok, result} = PhoenixMicro.RPC.call("math.sum", [1, 2, 3], timeout: 3_000)

      # Responder side — in a Consumer
      defmodule MathConsumer do
        use PhoenixMicro.Consumer
        topic "math.sum"

        def handle(%{payload: numbers, reply_to: reply_to, correlation_id: cid}, _ctx) do
          result = Enum.sum(numbers)
          PhoenixMicro.RPC.respond(reply_to, result, cid)
          :ok
        end
      end
  """

  use GenServer

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}

  @inbox_prefix "_inbox_"
  @default_timeout 5_000

  defstruct [
    :transport_mod,
    :inbox_topic,
    :subscription_ref,
    # %{correlation_id => {from, timer_ref}}
    pending: %{}
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Performs an RPC call to `topic` with `payload`.

  Options:
  - `:timeout` (integer, default 5000) — milliseconds to wait for response.
  - `:retry` (integer, default 0) — number of times to retry on timeout.
  - `:transport` (atom) — override the transport for this call.
  """
  @spec call(String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, :timeout} | {:error, term()}
  def call(topic, payload, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, Config.get(:default_timeout, @default_timeout))
    retries = Keyword.get(opts, :retry, 0)
    do_call(topic, payload, opts, retries, timeout)
  end

  @doc """
  Sends a response back to an RPC caller.
  Should be called from a consumer's `handle/2` when `message.reply_to` is set.
  """
  @spec respond(String.t(), term(), String.t()) :: :ok
  def respond(reply_to, result, correlation_id) when is_binary(reply_to) do
    GenServer.cast(__MODULE__, {:respond, reply_to, result, correlation_id})
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    transport_name = Keyword.get(opts, :transport, Config.get(:transport, :memory))
    transport_mod = Config.transport_module(transport_name)

    # Each node gets a unique inbox topic
    node_id = :erlang.phash2({node(), self()}, 1_000_000)
    inbox_topic = "#{@inbox_prefix}#{node_id}"

    handler = fn message ->
      GenServer.cast(__MODULE__, {:response_received, message})
      :ok
    end

    {:ok, sub_ref} = transport_mod.subscribe(inbox_topic, handler, [])

    state = %__MODULE__{
      transport_mod: transport_mod,
      inbox_topic: inbox_topic,
      subscription_ref: sub_ref,
      pending: %{}
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:call, topic, payload, opts}, from, state) do
    correlation_id = Message.generate_id()
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    message =
      Message.new(topic, payload,
        correlation_id: correlation_id,
        reply_to: state.inbox_topic
      )

    start = System.monotonic_time()
    Telemetry.rpc_request(topic, %{correlation_id: correlation_id})

    case state.transport_mod.publish(topic, message, opts) do
      :ok ->
        timer_ref = Process.send_after(self(), {:rpc_timeout, correlation_id}, timeout)

        new_pending =
          Map.put(state.pending, correlation_id, %{
            from: from,
            timer_ref: timer_ref,
            start: start,
            topic: topic
          })

        {:noreply, %{state | pending: new_pending}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_cast({:respond, reply_to, result, correlation_id}, state) do
    response_message = Message.new(reply_to, result, correlation_id: correlation_id)

    state.transport_mod.publish(reply_to, response_message, [])
    {:noreply, state}
  end

  @impl GenServer
  def handle_cast({:response_received, message}, state) do
    cid = message.correlation_id

    case Map.get(state.pending, cid) do
      nil ->
        Logger.warning("[RPC] Received response for unknown correlation_id: #{cid}")
        {:noreply, state}

      %{from: from, timer_ref: timer_ref, start: start, topic: topic} ->
        _cancelled = Process.cancel_timer(timer_ref)
        duration = System.monotonic_time() - start
        Telemetry.rpc_response(topic, %{correlation_id: cid, duration: duration})

        GenServer.reply(from, {:ok, message.payload})
        {:noreply, %{state | pending: Map.delete(state.pending, cid)}}
    end
  end

  @impl GenServer
  def handle_info({:rpc_timeout, correlation_id}, state) do
    case Map.get(state.pending, correlation_id) do
      nil ->
        {:noreply, state}

      %{from: from, topic: topic} ->
        Logger.warning("[RPC] Timeout for correlation_id #{correlation_id} on #{topic}")
        Telemetry.rpc_timeout(topic, %{correlation_id: correlation_id})
        GenServer.reply(from, {:error, :timeout})
        {:noreply, %{state | pending: Map.delete(state.pending, correlation_id)}}
    end
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp do_call(topic, payload, opts, retries_left, timeout) do
    case GenServer.call(__MODULE__, {:call, topic, payload, opts}, timeout + 500) do
      {:ok, result} ->
        {:ok, result}

      {:error, :timeout} when retries_left > 0 ->
        Logger.info("[RPC] Retrying call to #{topic}, #{retries_left} attempts remaining")
        do_call(topic, payload, opts, retries_left - 1, timeout)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
