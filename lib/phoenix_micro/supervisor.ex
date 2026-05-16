defmodule PhoenixMicro.Supervisor do
  @moduledoc """
  Root supervisor for the `phoenix_micro` OTP application.

  ## Supervision tree

      PhoenixMicro.Supervisor (one_for_one)
      ├── Registry
      ├── CircuitBreaker.Store
      ├── Schema.Registry
      ├── Phoenix.MetricsStore
      ├── Transport.Memory (always — no connection supervisor needed)
      ├── ConnectionSupervisor(:rabbitmq | :kafka | :nats | :redis_streams)
      │     ├── Transport.* (connection GenServer)
      │     └── WorkerPool (bounded task pool for message processing)
      ├── Producer
      ├── RPC
      ├── ConsumerManager (DynamicSupervisor)
      │     └── Pipeline (Broadway) per consumer
      └── Saga.Supervisor (DynamicSupervisor)

  Each real transport gets its own `ConnectionSupervisor` using a
  `:rest_for_one` strategy: if the connection dies, its worker pool
  restarts too (workers hold channel references).
  """

  use Supervisor

  alias PhoenixMicro.Config
  alias PhoenixMicro.Transport.ConnectionSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    config = Config.get()
    transport = Keyword.get(config, :transport, :memory)
    transports_config = Keyword.get(config, :transports, [])

    children =
      [
        # Process registry — used by Consumer.Worker via_name and Saga
        {Registry, keys: :unique, name: PhoenixMicro.Registry},

        # Circuit breaker ETS store — starts before any consumers
        PhoenixMicro.Middleware.CircuitBreaker.Store,

        # Schema registry — self-populated by @after_compile hooks
        PhoenixMicro.Schema.Registry,

        # Metrics ring-buffer store for LiveDashboard
        PhoenixMicro.Phoenix.MetricsStore,

        # Memory transport — always available, no external connection needed
        {PhoenixMicro.Transport.Memory, Keyword.get(transports_config, :memory, [])},

        # Per-transport ConnectionSupervisor (connection + worker pool)
        transport_child_spec(transport, transports_config),

        # Producer — batching GenServer
        {PhoenixMicro.Producer, []},

        # RPC — correlation-ID manager
        {PhoenixMicro.RPC, []},

        # Consumer DynamicSupervisor (Broadway pipeline per consumer)
        {PhoenixMicro.Supervisor.ConsumerManager, []},

        # Saga DynamicSupervisor — each saga run is an isolated process
        {PhoenixMicro.Saga.Supervisor, []}
      ]
      |> Enum.reject(&is_nil/1)

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ---------------------------------------------------------------------------
  # Per-transport child specs — wrap real transports in ConnectionSupervisor
  # ---------------------------------------------------------------------------

  defp transport_child_spec(:memory, _opts), do: nil

  defp transport_child_spec(:rabbitmq, transports_config) do
    if Code.ensure_loaded?(AMQP) do
      connection_supervisor_spec(
        :rabbitmq,
        PhoenixMicro.Transport.RabbitMQ,
        Keyword.get(transports_config, :rabbitmq, [])
      )
    else
      warn_missing(:rabbitmq, "amqp")
    end
  end

  defp transport_child_spec(:kafka, transports_config) do
    # Pure-Elixir Kafka transport — no external dependency required.
    # Implements the Kafka binary wire protocol natively over :gen_tcp.
    # No kafka_ex, no :brod, no crc32cer, no C compiler needed.
    connection_supervisor_spec(
      :kafka,
      PhoenixMicro.Transport.Kafka,
      Keyword.get(transports_config, :kafka, [])
    )
  end

  defp transport_child_spec(:nats, transports_config) do
    if Code.ensure_loaded?(Gnat) do
      connection_supervisor_spec(
        :nats,
        PhoenixMicro.Transport.NATS,
        Keyword.get(transports_config, :nats, [])
      )
    else
      warn_missing(:nats, "gnat", "Add {:gnat, \"~> 1.8\"} to deps.")
    end
  end

  defp transport_child_spec(:redis_streams, transports_config) do
    if Code.ensure_loaded?(Redix) do
      connection_supervisor_spec(
        :redis_streams,
        PhoenixMicro.Transport.RedisStreams,
        Keyword.get(transports_config, :redis_streams, [])
      )
    else
      warn_missing(:redis_streams, "redix", "Add {:redix, \"~> 1.3\"} to deps.")
    end
  end

  defp transport_child_spec(mod, _tcfg) when is_atom(mod) do
    # Custom transport module — no ConnectionSupervisor wrapping
    {mod, []}
  end

  defp connection_supervisor_spec(name, module, transport_config) do
    pool_size = Keyword.get(transport_config, :pool_size, 10)

    %{
      id: {ConnectionSupervisor, name},
      start:
        {ConnectionSupervisor, :start_link,
         [
           [
             transport_name: name,
             transport_module: module,
             transport_config: transport_config,
             pool_size: pool_size
           ]
         ]},
      type: :supervisor,
      restart: :permanent
    }
  end

  defp warn_missing(transport, dep, hint \\ "") do
    require Logger

    msg = "[PhoenixMicro] :#{transport} transport configured but :#{dep} is not loaded. #{hint}"

    Logger.warning(msg)
    nil
  end
end

defmodule PhoenixMicro.Supervisor.ConsumerManager do
  @moduledoc """
  DynamicSupervisor that manages all registered consumer worker processes.
  Consumers are registered either:
  1. Statically via `PhoenixMicro.register_consumer/1` at app start.
  2. Dynamically at runtime via `PhoenixMicro.Supervisor.ConsumerManager.start_consumer/1`.
  """

  use DynamicSupervisor

  require Logger

  alias PhoenixMicro.Consumer

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Starts the processing pipeline for the given consumer module.

  Chooses between `PhoenixMicro.Pipeline` (Broadway-backed, default) and
  the legacy `PhoenixMicro.Consumer.Worker` (Task-based) depending on the
  consumer's `pipeline` setting:

      pipeline :broadway   # default — full backpressure, batch support
      pipeline :simple     # legacy — Task.start dispatch

  Idempotent — if the consumer is already running, returns `{:ok, pid}`.
  """
  @spec start_consumer(module()) :: DynamicSupervisor.on_start_child()
  def start_consumer(consumer_module) when is_atom(consumer_module) do
    cfg = Consumer.config(consumer_module)
    spec = build_child_spec(consumer_module, cfg)

    case DynamicSupervisor.start_child(__MODULE__, spec) do
      {:ok, started_pid} ->
        mode = Map.get(cfg, :pipeline, :broadway)

        Logger.info(
          "[ConsumerManager] Started #{mode} consumer #{inspect(consumer_module)} pid=#{inspect(started_pid)}"
        )

        {:ok, started_pid}

      {:error, {:already_started, existing_pid}} ->
        {:ok, existing_pid}

      {:error, reason} ->
        Logger.error("[ConsumerManager] Failed to start #{inspect(consumer_module)}: #{inspect(reason)}")

        {:error, reason}
    end
  end

  @doc """
  Stops a running consumer (works for both Broadway and Worker modes).
  """
  @spec stop_consumer(module()) :: :ok | {:error, :not_found}
  def stop_consumer(consumer_module) do
    # Broadway pipelines register under their own name; Workers use Registry
    pipeline_name = Module.safe_concat([PhoenixMicro.Pipeline, consumer_module])

    case Process.whereis(pipeline_name) do
      nil ->
        # Fall back to Registry lookup for legacy Worker mode
        case Registry.lookup(
               PhoenixMicro.Registry,
               {PhoenixMicro.Consumer.Worker, consumer_module}
             ) do
          [{pid, _value}] ->
            DynamicSupervisor.terminate_child(__MODULE__, pid)

          [] ->
            {:error, :not_found}
        end

      pid ->
        DynamicSupervisor.terminate_child(__MODULE__, pid)
    end
  end

  @doc """
  Returns a list of all running consumer modules.
  """
  @spec running_consumers() :: [module()]
  def running_consumers do
    DynamicSupervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _mods} -> id end)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp build_child_spec(consumer_module, %{pipeline: :simple}) do
    %{
      id: consumer_module,
      start: {Consumer, :start_link, [consumer_module, []]},
      restart: :permanent,
      type: :worker
    }
  end

  defp build_child_spec(consumer_module, _cfg) do
    # Broadway mode (default)
    %{
      id: {PhoenixMicro.Pipeline, consumer_module},
      start: {PhoenixMicro.Pipeline, :start_link, [consumer_module, []]},
      restart: :permanent,
      type: :supervisor
    }
  end
end
