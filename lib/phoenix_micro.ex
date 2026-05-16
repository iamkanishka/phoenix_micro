defmodule PhoenixMicro do
  @moduledoc """
  PhoenixMicro — a production-grade microservices toolkit for Elixir/Phoenix.

  Phoenix Microservices, built natively for OTP and the BEAM VM.

  ## Quick start

      # 1. Configure in config/config.exs
      config :phoenix_micro,
        transport: :rabbitmq,
        consumers: [MyApp.Payments.CreatedConsumer],
        transports: [
          rabbitmq: [url: "amqp://localhost"]
        ]

      # 2. Define a consumer
      defmodule MyApp.Payments.CreatedConsumer do
        use PhoenixMicro.Consumer

        topic "payments.created"
        concurrency 5
        retry max_attempts: 3

        @impl PhoenixMicro.Consumer
        def handle(message, _ctx) do
          Logger.info("Received payload: \#{inspect(message.payload)}\")
          :ok
        end
      end

      # 3. Publish from anywhere
      PhoenixMicro.publish("payments.created", %{amount: 100, currency: "USD"})

      # 4. RPC — two calling conventions
      {:ok, result} = PhoenixMicro.rpc("math.sum", [1, 2, 3])
      {:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3])

  ## Architecture

      PhoenixMicro.Application
        └── PhoenixMicro.Supervisor (one_for_one)
              ├── Registry
              ├── Transport.Memory
              ├── Transport.* (configured)
              ├── Producer
              ├── RPC
              └── ConsumerManager (DynamicSupervisor)
                    └── Pipeline (Broadway) per consumer
  """

  alias PhoenixMicro.{Config, Producer, RPC}
  alias PhoenixMicro.Supervisor.ConsumerManager

  # ---------------------------------------------------------------------------
  # Publishing
  # ---------------------------------------------------------------------------

  @doc """
  Publishes a message to `topic` asynchronously (fire-and-forget).

  Options: `:transport`, `:headers`, `:correlation_id`.
  """
  @spec publish(String.t(), term(), keyword()) :: :ok
  defdelegate publish(topic, payload, opts \\ []), to: Producer

  @doc """
  Publishes to `topic` synchronously. Returns `:ok` or `{:error, reason}`.
  """
  @spec publish_sync(String.t(), term(), keyword()) :: :ok | {:error, term()}
  defdelegate publish_sync(topic, payload, opts \\ []), to: Producer

  @doc """
  Publishes a batch of `[{topic, payload}]` tuples.
  """
  @spec publish_batch([{String.t(), term()}], keyword()) :: :ok
  defdelegate publish_batch(messages, opts \\ []), to: Producer

  # ---------------------------------------------------------------------------
  # RPC — supports both rpc(topic, payload, opts) and rpc(service, pattern, payload)
  # ---------------------------------------------------------------------------

  @doc """
  Synchronous RPC call. Supports two signatures:

      # topic + payload form
      {:ok, result} = PhoenixMicro.rpc("math.sum", [1, 2, 3])

      # service + pattern + payload form (spec-compliant)
      {:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3])

      # with options
      {:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3], timeout: 3_000)

  Options: `:timeout` (ms, default 5000), `:retry` (attempts, default 0).
  """
  @spec rpc(String.t(), term(), keyword() | term()) :: {:ok, term()} | {:error, term()}
  def rpc(topic, payload, opts \\ [])

  def rpc(topic, payload, opts) when is_binary(topic) and is_list(opts) do
    RPC.call(topic, payload, opts)
  end

  # rpc(service, pattern, payload) — pattern is binary, payload is not opts list
  def rpc(service, pattern, payload)
      when is_binary(service) and is_binary(pattern) do
    RPC.call("#{service}.#{pattern}", payload, [])
  end

  @doc """
  4-argument RPC: `rpc(service, pattern, payload, opts)`.

      {:ok, result} = PhoenixMicro.rpc("math", "sum", [1, 2, 3], timeout: 3_000)
  """
  @spec rpc(String.t(), String.t(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def rpc(service, pattern, payload, opts)
      when is_binary(service) and is_binary(pattern) and is_list(opts) do
    RPC.call("#{service}.#{pattern}", payload, opts)
  end

  # ---------------------------------------------------------------------------
  # Consumer management
  # ---------------------------------------------------------------------------

  @doc "Dynamically registers and starts a consumer module."
  @spec register_consumer(module()) :: {:ok, pid()} | {:error, term()}
  defdelegate register_consumer(module), to: ConsumerManager, as: :start_consumer

  @doc "Stops a running consumer."
  @spec deregister_consumer(module()) :: :ok | {:error, :not_found}
  defdelegate deregister_consumer(module), to: ConsumerManager, as: :stop_consumer

  @doc "Returns all currently running consumer modules."
  @spec consumers() :: [module()]
  defdelegate consumers(), to: ConsumerManager, as: :running_consumers

  @doc "Returns the active transport module."
  @spec transport() :: module()
  def transport, do: Config.active_transport()
end
