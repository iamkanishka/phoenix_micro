defmodule PhoenixMicro.Message do
  @moduledoc """
  The canonical message envelope for all inter-service communication.

  Every message flowing through `phoenix_micro` — whether published to a broker,
  consumed from a queue, or passed through an RPC call — is wrapped in this struct.

  ## Fields

  * `:id`            — UUID v4; assigned at publish time; used for idempotency tracking.
  * `:topic`         — The routing key / topic name (e.g. `"payments.created"`).
  * `:payload`       — The decoded message body. Any term.
  * `:headers`       — Arbitrary key-value metadata map (string keys).
  * `:attempt`       — Delivery attempt counter; starts at 1, incremented on retry.
  * `:timestamp`     — UTC `DateTime` of original publish time.
  * `:reply_to`      — Optional reply-topic for RPC correlation.
  * `:correlation_id`— Optional ID linking an RPC request to its response.
  * `:raw`           — The raw broker-native message (transport-specific). Opaque.
  * `:acked?`        — Whether this message has been ack'd/nack'd by the consumer.
  * `:metadata`      — Transport-specific extra data (partition, offset, routing key, …).
  """

  @enforce_keys [:id, :topic, :payload, :timestamp]

  alias PhoenixMicro.Utils.ID

  defstruct [
    :id,
    :topic,
    :payload,
    :reply_to,
    :correlation_id,
    :raw,
    headers: %{},
    attempt: 1,
    acked?: false,
    metadata: %{},
    timestamp: nil
  ]

  @type headers :: %{String.t() => String.t()}
  @type metadata :: %{atom() => term()}

  @type t :: %__MODULE__{
          id: String.t(),
          topic: String.t(),
          payload: term(),
          headers: headers(),
          attempt: pos_integer(),
          timestamp: DateTime.t(),
          reply_to: String.t() | nil,
          correlation_id: String.t() | nil,
          raw: term(),
          acked?: boolean(),
          metadata: metadata()
        }

  @doc """
  Creates a new message with a generated UUID and current UTC timestamp.

  ## Examples

      iex> msg = PhoenixMicro.Message.new("payments.created", %{amount: 100})
      iex> msg.topic
      "payments.created"
      iex> msg.attempt
      1
  """
  @spec new(String.t(), term(), keyword()) :: t()
  def new(topic, payload, opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :id, generate_id()),
      topic: topic,
      payload: payload,
      headers: Keyword.get(opts, :headers, %{}),
      attempt: Keyword.get(opts, :attempt, 1),
      timestamp: Keyword.get(opts, :timestamp, DateTime.utc_now()),
      reply_to: Keyword.get(opts, :reply_to),
      correlation_id: Keyword.get(opts, :correlation_id),
      raw: Keyword.get(opts, :raw),
      acked?: false,
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  @doc """
  Returns a new message with the attempt counter incremented.
  """
  @spec increment_attempt(t()) :: t()
  def increment_attempt(%__MODULE__{} = msg) do
    %{msg | attempt: msg.attempt + 1}
  end

  @doc """
  Marks the message as acknowledged.
  """
  @spec ack(t()) :: t()
  def ack(%__MODULE__{} = msg), do: %{msg | acked?: true}

  @doc """
  Merges additional metadata into the message.
  """
  @spec put_metadata(t(), metadata()) :: t()
  def put_metadata(%__MODULE__{} = msg, extra) when is_map(extra) do
    %{msg | metadata: Map.merge(msg.metadata, extra)}
  end

  @doc """
  Adds or updates a header.
  """
  @spec put_header(t(), String.t(), String.t()) :: t()
  def put_header(%__MODULE__{} = msg, key, value) when is_binary(key) and is_binary(value) do
    %{msg | headers: Map.put(msg.headers, key, value)}
  end

  alias PhoenixMicro.Utils.ID

  @doc false
  @spec generate_id() :: String.t()
  def generate_id, do: ID.uuid4()
end
