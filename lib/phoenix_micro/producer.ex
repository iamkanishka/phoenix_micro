defmodule PhoenixMicro.Producer do
  @moduledoc """
  High-level message producer with batching, async fire-and-forget,
  and sync-with-ack publishing.

  ## Usage

      # Async fire-and-forget
      PhoenixMicro.Producer.publish("payments.created", %{amount: 100})

      # Sync with confirmation
      :ok = PhoenixMicro.Producer.publish_sync("payments.created", %{amount: 100})

      # Batch publish
      messages = [
        {"payments.created", %{amount: 100}},
        {"payments.created", %{amount: 200}}
      ]
      PhoenixMicro.Producer.publish_batch(messages)
  """

  use GenServer

  require Logger

  alias PhoenixMicro.{Config, Message, Telemetry}
  alias PhoenixMicro.Utils.Encoding

  # ms
  @default_batch_interval 50
  @default_batch_size 100

  defstruct [
    :transport_mod,
    :batch_timer,
    batch: [],
    batch_size: @default_batch_size,
    batch_interval: @default_batch_interval
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Publishes a message asynchronously (fire-and-forget).
  Returns immediately; does not wait for broker acknowledgment.
  """
  @spec publish(String.t(), term(), keyword()) :: :ok
  def publish(topic, payload, opts \\ []) do
    GenServer.cast(__MODULE__, {:publish, topic, payload, opts})
  end

  @doc """
  Publishes a message synchronously, waiting for broker confirmation.
  Returns `:ok` or `{:error, reason}`.
  """
  @spec publish_sync(String.t(), term(), keyword()) :: :ok | {:error, term()}
  def publish_sync(topic, payload, opts \\ []) do
    GenServer.call(
      __MODULE__,
      {:publish_sync, topic, payload, opts},
      Keyword.get(opts, :timeout, 5_000)
    )
  end

  @doc """
  Publishes multiple messages, automatically batching them.
  """
  @spec publish_batch([{String.t(), term()}], keyword()) :: :ok
  def publish_batch(messages, opts \\ []) when is_list(messages) do
    GenServer.cast(__MODULE__, {:publish_batch, messages, opts})
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

    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    batch_interval = Keyword.get(opts, :batch_interval, @default_batch_interval)

    state = %__MODULE__{
      transport_mod: transport_mod,
      batch: [],
      batch_size: batch_size,
      batch_interval: batch_interval
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:publish, topic, payload, opts}, state) do
    message = build_message(topic, payload, opts)
    new_state = enqueue(message, state)
    {:noreply, maybe_flush(new_state)}
  end

  @impl GenServer
  def handle_cast({:publish_batch, messages, opts}, state) do
    built =
      Enum.map(messages, fn {topic, payload} ->
        build_message(topic, payload, opts)
      end)

    new_state = Enum.reduce(built, state, &enqueue/2)
    {:noreply, maybe_flush(new_state)}
  end

  @impl GenServer
  def handle_call({:publish_sync, topic, payload, opts}, _from, state) do
    message = build_message(topic, payload, opts)
    result = state.transport_mod.publish(topic, message, opts)
    {:reply, result, state}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    {:noreply, flush(state)}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp build_message(topic, payload, opts) do
    headers = Keyword.get(opts, :headers, %{})
    correlation_id = Keyword.get(opts, :correlation_id)
    reply_to = Keyword.get(opts, :reply_to)

    # Serialise payload to binary using the configured serializer (default: JSON).
    # The raw binary is stored in the message; transports publish it as-is.
    serialized =
      case Encoding.encode(payload) do
        {:ok, bin} -> bin
        {:error, _enc_err} -> Jason.encode!(payload)
      end

    content_type = Encoding.content_type()

    Message.new(topic, payload,
      headers: Map.merge(headers, %{"content-type" => content_type}),
      correlation_id: correlation_id,
      reply_to: reply_to,
      metadata: %{serialized: serialized}
    )
  end

  defp enqueue(message, %{batch: batch, batch_size: _max, batch_interval: interval} = state) do
    new_batch = [message | batch]

    timer =
      if state.batch_timer == nil do
        Process.send_after(self(), :flush, interval)
      else
        state.batch_timer
      end

    %{state | batch: new_batch, batch_timer: timer}
  end

  defp maybe_flush(%{batch: batch, batch_size: max} = state) when length(batch) >= max do
    flush(state)
  end

  defp maybe_flush(state), do: state

  defp flush(%{batch: []} = state), do: state

  defp flush(%{batch: batch, transport_mod: transport_mod} = state) do
    _timer_cancelled =
      if state.batch_timer do
        Process.cancel_timer(state.batch_timer)
      end

    # Publish oldest-first
    batch
    |> Enum.reverse()
    |> Enum.each(fn message ->
      case transport_mod.publish(message.topic, message, []) do
        :ok ->
          Telemetry.message_published(message.topic, %{batched: true})

        {:error, reason} ->
          Logger.error("[Producer] Failed to publish #{message.id}: #{inspect(reason)}")
          Telemetry.message_failed(message.topic, %{reason: reason, phase: :publish})
      end
    end)

    %{state | batch: [], batch_timer: nil}
  end
end
