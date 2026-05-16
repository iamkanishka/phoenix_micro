defmodule PhoenixMicro.Transport.ConnectionSupervisor do
  @moduledoc """
  A `Supervisor` that owns a single transport's connection process and its
  associated worker pool.

  One `ConnectionSupervisor` is started per configured transport:

      TransportSupervisor (one_for_one root)
      └── ConnectionSupervisor(:rabbitmq)   ← this module
            ├── Transport.RabbitMQ           ← connection GenServer
            └── WorkerPool(:rabbitmq_pool)   ← message processing pool

  The `:rest_for_one` strategy means: if the connection dies, the worker
  pool is restarted too (workers hold references to the connection channel).
  If only the pool dies, the connection is left intact.
  """

  use Supervisor

  alias PhoenixMicro.Utils.WorkerPool

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    transport_name = Keyword.fetch!(opts, :transport_name)
    Supervisor.start_link(__MODULE__, opts, name: via(transport_name))
  end

  @impl Supervisor
  def init(opts) do
    transport_name = Keyword.fetch!(opts, :transport_name)
    transport_module = Keyword.fetch!(opts, :transport_module)
    transport_config = Keyword.get(opts, :transport_config, [])
    pool_size = Keyword.get(opts, :pool_size, 10)

    pool_name = pool_name(transport_name)

    children = [
      # The transport connection GenServer
      {transport_module, transport_config},

      # Bounded worker pool for this transport's message handlers
      {WorkerPool, name: pool_name, max_concurrency: pool_size}
    ]

    # rest_for_one: if connection dies, pool restarts too
    Supervisor.init(children, strategy: :rest_for_one)
  end

  # ---------------------------------------------------------------------------
  # Public helpers
  # ---------------------------------------------------------------------------

  @doc "Returns the worker pool name for a given transport."
  @spec pool_name(atom()) :: atom()
  def pool_name(transport_name) do
    String.to_existing_atom("phoenix_micro_#{transport_name}_pool")
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp via(transport_name) do
    :erlang.binary_to_existing_atom("phoenix_micro_transport_sup_#{transport_name}", :utf8)
  end
end
