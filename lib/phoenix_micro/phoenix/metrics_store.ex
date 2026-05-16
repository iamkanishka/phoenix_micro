defmodule PhoenixMicro.Phoenix.MetricsStore do
  @moduledoc """
  In-process ring-buffer store for PhoenixMicro Telemetry metrics.

  Subscribes to all `[:phoenix_micro, ...]` events and maintains the last
  N data points per metric. The LiveDashboard page reads from this store
  to render real-time charts without hitting a time-series database.

  Start it in your supervision tree before `PhoenixMicro.Application`:

      children = [
        PhoenixMicro.Phoenix.MetricsStore,
        # ...
      ]

  Or rely on `PhoenixMicro.Application` starting it automatically when
  `:phoenix_live_dashboard` is detected.
  """

  use GenServer

  @table :phoenix_micro_metrics
  # 5 minutes at 1-second resolution
  @ring_size 300

  # ---------------------------------------------------------------------------
  # Public API
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

  @doc "Returns the last N data points for a given metric key."
  @spec get(atom()) :: [map()]
  def get(metric_key) do
    case :ets.lookup(@table, metric_key) do
      [{^metric_key, ring}] -> :queue.to_list(ring)
      [] -> []
    end
  end

  @doc "Returns a snapshot of all metrics."
  @spec all() :: %{atom() => [map()]}
  def all do
    :ets.tab2list(@table)
    |> Map.new(fn {k, ring} -> {k, :queue.to_list(ring)} end)
  end

  @doc "Returns the latest single value for a metric (most recent data point)."
  @spec latest(atom()) :: map() | nil
  def latest(metric_key) do
    case :ets.lookup(@table, metric_key) do
      [{^metric_key, ring}] ->
        case :queue.peek_r(ring) do
          {:value, item} -> item
          :empty -> nil
        end

      [] ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(_opts) do
    _table = :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    _attach = attach_telemetry()
    {:ok, %{}}
  end

  @impl GenServer
  def handle_cast({:record, metric_key, data_point}, state) do
    ring =
      case :ets.lookup(@table, metric_key) do
        [{^metric_key, existing}] -> existing
        [] -> :queue.new()
      end

    new_ring =
      if :queue.len(ring) >= @ring_size do
        ring |> :queue.drop() |> :queue.in(data_point)
      else
        :queue.in(data_point, ring)
      end

    :ets.insert(@table, {metric_key, new_ring})
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Telemetry attachment
  # ---------------------------------------------------------------------------

  defp attach_telemetry do
    events = [
      {[:phoenix_micro, :message, :received], :messages_received},
      {[:phoenix_micro, :message, :processed], :messages_processed},
      {[:phoenix_micro, :message, :failed], :messages_failed},
      {[:phoenix_micro, :message, :published], :messages_published},
      {[:phoenix_micro, :rpc, :request], :rpc_requests},
      {[:phoenix_micro, :rpc, :response], :rpc_responses},
      {[:phoenix_micro, :rpc, :timeout], :rpc_timeouts},
      {[:phoenix_micro, :pipeline, :demand], :pipeline_demand},
      {[:phoenix_micro, :pipeline, :enqueued], :pipeline_enqueued},
      {[:phoenix_micro, :pipeline, :buffer_full], :pipeline_buffer_full},
      {[:phoenix_micro, :circuit_breaker, :tripped], :cb_tripped},
      {[:phoenix_micro, :circuit_breaker, :reset], :cb_reset},
      {[:phoenix_micro, :circuit_breaker, :rejected], :cb_rejected},
      {[:phoenix_micro, :saga, :started], :saga_started},
      {[:phoenix_micro, :saga, :completed], :saga_completed},
      {[:phoenix_micro, :saga, :compensated], :saga_compensated},
      {[:phoenix_micro, :saga, :fatal], :saga_fatal}
    ]

    :telemetry.attach_many(
      "phoenix_micro_metrics_store",
      Enum.map(events, &elem(&1, 0)),
      &__MODULE__.handle_event/4,
      Map.new(events)
    )
  end

  @doc false
  def handle_event(event, measurements, metadata, event_map) do
    metric_key = Map.get(event_map, event)

    if metric_key do
      data_point = %{
        t: System.system_time(:millisecond),
        measurements: measurements,
        metadata: sanitise_metadata(metadata)
      }

      GenServer.cast(__MODULE__, {:record, metric_key, data_point})
    end
  end

  defp sanitise_metadata(meta) do
    meta
    |> Enum.reject(fn {_k, v} -> is_pid(v) or is_reference(v) or is_function(v) end)
    |> Map.new(fn {k, v} -> {k, safe_value(v)} end)
  end

  defp safe_value(v) when is_atom(v), do: to_string(v)
  defp safe_value(v) when is_binary(v) or is_number(v) or is_boolean(v), do: v
  defp safe_value(v), do: inspect(v)
end
