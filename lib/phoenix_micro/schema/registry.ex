defmodule PhoenixMicro.Schema.Registry do
  @moduledoc """
  ETS-backed registry mapping topic names to their schema modules.

  Schemas self-register via `@after_compile` hooks when you `use PhoenixMicro.Schema`.
  You can also register programmatically:

      PhoenixMicro.Schema.Registry.register(MyApp.Events.PaymentCreated)

  The registry stores multiple versions per topic so you can query the history:

      PhoenixMicro.Schema.Registry.versions("payments.created")
      # => [{1, MyApp.Events.PaymentCreatedV1}, {2, MyApp.Events.PaymentCreated}]
  """

  use GenServer

  @table :phoenix_micro_schema_registry

  # ---------------------------------------------------------------------------
  # Supervision
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Registers a schema module under its declared topic and version.
  Safe to call multiple times (idempotent).
  """
  @spec register(module()) :: :ok
  def register(schema_module) do
    topic = schema_module.topic()
    version = schema_module.schema_version()

    if Process.whereis(__MODULE__) do
      GenServer.call(__MODULE__, {:register, topic, schema_module, version})
    else
      # Registry not yet started — silently ignore (happens during compilation)
      :ok
    end
  end

  @doc "Returns all registered schema modules (deduplicated)."
  @spec all() :: [module()]
  def all do
    if :ets.whereis(@table) != :undefined do
      @table
      |> :ets.tab2list()
      |> Enum.map(fn {_topic, module, _version} -> module end)
      |> Enum.uniq()
    else
      []
    end
  end

  @doc """
  Returns the latest-version schema module for a topic.
  Returns `{:error, :not_found}` if no schema is registered.
  """
  @spec lookup(String.t()) :: {:ok, module()} | {:error, :not_found}
  def lookup(topic) do
    if :ets.whereis(@table) != :undefined do
      case :ets.match_object(@table, {topic, :_, :_}) do
        [] ->
          {:error, :not_found}

        matches ->
          {_topic, module, _version} = Enum.max_by(matches, fn {_t, _m, version} -> version end)
          {:ok, module}
      end
    else
      {:error, :not_found}
    end
  end

  @doc """
  Returns all registered `{version, module}` pairs for a topic, oldest first.
  """
  @spec versions(String.t()) :: [{pos_integer(), module()}]
  def versions(topic) do
    if :ets.whereis(@table) != :undefined do
      @table
      |> :ets.match_object({topic, :_, :_})
      |> Enum.map(fn {_t, mod, ver} -> {ver, mod} end)
      |> Enum.sort_by(&elem(&1, 0))
    else
      []
    end
  end

  @doc "Removes all entries (useful in tests)."
  @spec clear() :: :ok
  def clear do
    if :ets.whereis(@table) != :undefined do
      :ets.delete_all_objects(@table)
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(_opts) do
    _table = :ets.new(@table, [:named_table, :bag, :public, read_concurrency: true])
    {:ok, %{}}
  end

  @impl GenServer
  def handle_call({:register, topic, module, version}, _from, state) do
    :ets.match_delete(@table, {topic, module, version})
    :ets.insert(@table, {topic, module, version})
    {:reply, :ok, state}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

end
