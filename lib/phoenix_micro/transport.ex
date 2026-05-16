defmodule PhoenixMicro.Transport do
  @moduledoc """
  The behaviour that every transport adapter must implement.

  A transport is an OTP process (GenServer or similar) that:

  1. Maintains a connection to the external broker.
  2. Publishes messages to topics/queues/subjects.
  3. Subscribes consumers to topics and delivers messages.
  4. Acknowledges or negatively-acknowledges messages.
  5. Handles reconnection transparently.

  ## Implementing a custom transport

      defmodule MyApp.Transport.SQS do
        @behaviour PhoenixMicro.Transport
        use GenServer

        @impl PhoenixMicro.Transport
        def connect(config) do
          # Return {:ok, state} where state is whatever your GenServer needs
        end

        # ... implement all callbacks
      end

  Register it in config:

      config :phoenix_micro,
        transport: MyApp.Transport.SQS,
        transports: [MyApp.Transport.SQS: [region: "us-east-1", ...]]
  """

  alias PhoenixMicro.Message

  @type config :: keyword()
  @type state :: term()
  @type topic :: String.t()
  @type handler :: (Message.t() -> :ok | {:error, term()})
  @type subscription_opts :: keyword()
  @type publish_opts :: keyword()

  # ---------------------------------------------------------------------------
  # Callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Establishes a connection to the broker.
  Called during transport process `init/1`.
  Must return `{:ok, state}` where `state` is carried in the GenServer state.
  """
  @callback connect(config()) :: {:ok, state()} | {:error, term()}

  @doc """
  Publishes a message to the given topic.
  `opts` may include `:headers`, `:partition_key`, `:priority`, etc.
  """
  @callback publish(topic(), Message.t(), publish_opts()) :: :ok | {:error, term()}

  @doc """
  Subscribes to the given topic pattern and delivers messages to `handler`.
  Returns `{:ok, subscription_ref}` where the ref can be used to unsubscribe.
  """
  @callback subscribe(topic(), handler(), subscription_opts()) ::
              {:ok, reference()} | {:error, term()}

  @doc """
  Unsubscribes a previously established subscription.
  """
  @callback unsubscribe(reference(), state()) :: :ok

  @doc """
  Acknowledges successful processing of a message.
  """
  @callback ack(Message.t(), state()) :: :ok

  @doc """
  Negatively acknowledges a message, indicating processing failure.
  `reason` is used for logging and DLQ routing.
  """
  @callback nack(Message.t(), reason :: term(), state()) :: :ok

  @doc """
  Cleanly closes the connection to the broker.
  """
  @callback disconnect(state()) :: :ok

  @doc """
  Returns the current connection status.
  """
  @callback status(state()) :: :connected | :disconnected | :reconnecting

  # ---------------------------------------------------------------------------
  # Optional callbacks
  # ---------------------------------------------------------------------------

  @optional_callbacks [unsubscribe: 2, status: 1]

  # ---------------------------------------------------------------------------
  # Shared helpers used by all transport implementations
  # ---------------------------------------------------------------------------

  @doc """
  Dispatches `message` through the middleware chain and into the `handler`.
  Used internally by transport implementations to invoke consumer handlers.
  """
  @spec dispatch(Message.t(), handler(), [module()]) :: :ok | {:error, term()}
  def dispatch(message, handler, middlewares \\ []) do
    chain = build_chain(middlewares, handler)
    chain.(message)
  end

  @doc false
  @spec build_chain([module()], handler()) :: handler()
  def build_chain([], handler), do: handler

  def build_chain([middleware | rest], handler) do
    next = build_chain(rest, handler)
    fn message -> middleware.call(message, next) end
  end
end
