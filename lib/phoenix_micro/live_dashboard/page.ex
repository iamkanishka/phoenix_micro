defmodule PhoenixMicro.LiveDashboard.Page do
  @moduledoc """
  A `Phoenix.LiveDashboard` page that shows real-time PhoenixMicro metrics.

  Requires `phoenix_live_dashboard` in your application's deps:

      {:phoenix_live_dashboard, "~> 0.8"}

  Add to your router:

      live_dashboard "/dashboard",
        additional_pages: [phoenix_micro: PhoenixMicro.LiveDashboard.Page]
  """

  # All PageBuilder callbacks below are guarded with Code.ensure_loaded?/1 at
  # runtime so this module compiles cleanly whether or not phoenix_live_dashboard
  # is installed. No compile-time @behaviour declaration is used — the router
  # discovers our callbacks by duck-typing when the dep is present.

  require Logger

  alias PhoenixMicro.Config
  alias PhoenixMicro.Middleware.CircuitBreaker.Store, as: CBStore
  alias PhoenixMicro.Phoenix.MetricsStore
  alias PhoenixMicro.Supervisor.ConsumerManager

  @refresh_interval 2_000

  # ---------------------------------------------------------------------------
  # PageBuilder callbacks — all guarded with Code.ensure_loaded? at runtime
  # so they compile cleanly whether or not phoenix_live_dashboard is present.
  # ---------------------------------------------------------------------------

  @doc false
  def menu_link(_page, _session), do: {:ok, "PhoenixMicro"}

  @doc false
  def init(opts) do
    # Required by Phoenix.LiveDashboard.PageBuilder since v0.7.
    # Return the opts unchanged — we store no page-level state.
    {:ok, opts}
  end

  @doc false
  def mount(params, session, socket) do
    if Code.ensure_loaded?(Phoenix.LiveDashboard.PageBuilder) and
         function_exported?(Phoenix.LiveDashboard.PageBuilder, :assign_defaults, 4) do
      socket =
        apply(Phoenix.LiveDashboard.PageBuilder, :assign_defaults, [
          socket,
          params,
          session,
          @refresh_interval
        ])

      {:ok, assign_metrics(socket)}
    else
      {:ok, socket}
    end
  rescue
    _e -> {:ok, socket}
  end

  @doc false
  def render(assigns) do
    if Code.ensure_loaded?(Phoenix.LiveView) do
      apply(__MODULE__, :render_dashboard, [assigns])
    else
      "<div>phoenix_live_dashboard not installed</div>"
    end
  rescue
    _e -> ""
  end

  @doc false
  def handle_info(:refresh, socket) do
    {:noreply, assign_metrics(socket)}
  end

  @doc false
  def handle_info(_msg, socket), do: {:noreply, socket}

  # ---------------------------------------------------------------------------
  # Public helper — callable without LiveDashboard installed
  # ---------------------------------------------------------------------------

  @spec collect_data() :: %{
          transport: %{active: String.t(), status: String.t()},
          message_metrics: %{
            received: non_neg_integer(),
            processed: non_neg_integer(),
            failed: non_neg_integer(),
            published: non_neg_integer(),
            avg_duration_ms: number()
          },
          rpc_metrics: %{
            requests: non_neg_integer(),
            timeouts: non_neg_integer(),
            p50_ms: number(),
            p95_ms: number(),
            p99_ms: number()
          },
          circuit_breakers: list(),
          consumers: list(),
          sagas: %{
            started: non_neg_integer(),
            completed: non_neg_integer(),
            compensated: non_neg_integer(),
            fatal: non_neg_integer()
          }
        }
  def collect_data do
    %{
      transport: collect_transport(),
      message_metrics: collect_message_metrics(),
      rpc_metrics: collect_rpc_metrics(),
      circuit_breakers: collect_circuit_breakers(),
      consumers: collect_consumers(),
      sagas: collect_sagas()
    }
  end

  # ---------------------------------------------------------------------------
  # Private — data collection
  # ---------------------------------------------------------------------------

  defp assign_metrics(socket) do
    data = collect_data()

    socket
    |> assign_safe(:transport, data.transport)
    |> assign_safe(:message_metrics, data.message_metrics)
    |> assign_safe(:rpc_metrics, data.rpc_metrics)
    |> assign_safe(:circuit_breakers, data.circuit_breakers)
    |> assign_safe(:consumers, data.consumers)
    |> assign_safe(:sagas, data.sagas)
  end

  defp assign_safe(socket, key, value) do
    apply(Phoenix.LiveView, :assign, [socket, key, value])
  rescue
    _e -> socket
  end

  @doc false
  def render_dashboard(assigns) do
    transport = Map.get(assigns, :transport, %{active: "unknown", status: "unknown"})
    consumers = Map.get(assigns, :consumers, [])
    cbs = Map.get(assigns, :circuit_breakers, [])

    consumer_rows =
      Enum.map_join(consumers, "\n", fn c ->
        "<tr><td>#{c.module}</td><td>#{c.topic}</td><td>#{c.concurrency}</td></tr>"
      end)

    cb_rows =
      Enum.map_join(cbs, "\n", fn cb ->
        color = if cb.state == "open", do: "red", else: "green"
        "<tr><td>#{cb.fuse}</td><td style='color:#{color}'>#{cb.state}</td></tr>"
      end)

    """
    <div style="font-family:sans-serif;padding:1rem;">
      <h2>PhoenixMicro</h2>
      <h3>Transport: #{transport.active} (#{transport.status})</h3>
      <h3>Consumers (#{length(consumers)})</h3>
      <table border="1" cellpadding="4">
        <tr><th>Module</th><th>Topic</th><th>Concurrency</th></tr>
        #{consumer_rows}
      </table>
      <h3>Circuit Breakers (#{length(cbs)})</h3>
      <table border="1" cellpadding="4">
        <tr><th>Fuse</th><th>State</th></tr>
        #{cb_rows}
      </table>
    </div>
    """
  end

  defp collect_transport do
    mod = Config.active_transport()
    name = mod |> Module.split() |> List.last() |> String.downcase()

    status =
      try do
        case mod.status(%{}) do
          :connected -> "connected"
          :disconnected -> "disconnected"
          _status -> "unknown"
        end
      rescue
        _e -> "unknown"
      end

    %{active: name, status: status}
  end

  defp collect_message_metrics do
    try do
      events = MetricsStore.all()
      received = Enum.count(events, &(&1.event == :message_received))
      processed = Enum.count(events, &(&1.event == :message_processed))
      failed = Enum.count(events, &(&1.event == :message_failed))
      published = Enum.count(events, &(&1.event == :message_published))

      durations =
        events
        |> Enum.filter(&(&1.event == :message_processed))
        |> Enum.map(& &1.duration)
        |> Enum.reject(&is_nil/1)

      avg_duration_ms =
        if durations != [] do
          div(Enum.sum(durations), length(durations))
          |> System.convert_time_unit(:native, :millisecond)
        else
          0
        end

      %{
        received: received,
        processed: processed,
        failed: failed,
        published: published,
        avg_duration_ms: avg_duration_ms
      }
    rescue
      _e -> %{received: 0, processed: 0, failed: 0, published: 0, avg_duration_ms: 0}
    end
  end

  defp collect_rpc_metrics do
    try do
      events = MetricsStore.all()
      requests = Enum.count(events, &(&1.event == :rpc_request))
      timeouts = Enum.count(events, &(&1.event == :rpc_timeout))

      latencies =
        events
        |> Enum.filter(&(&1.event == :rpc_response))
        |> Enum.map(& &1.duration)
        |> Enum.reject(&is_nil/1)
        |> Enum.sort()

      %{
        requests: requests,
        timeouts: timeouts,
        p50_ms: percentile(latencies, 50),
        p95_ms: percentile(latencies, 95),
        p99_ms: percentile(latencies, 99)
      }
    rescue
      _e -> %{requests: 0, timeouts: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0}
    end
  end

  defp collect_circuit_breakers do
    try do
      CBStore.all_states()
      |> Enum.map(fn
        {fuse, :closed, _window} -> %{fuse: fuse, state: "closed"}
        {fuse, :open, opened_at} -> %{fuse: fuse, state: "open", opened_at_ms: opened_at}
        {fuse, :half_open, _window} -> %{fuse: fuse, state: "half_open"}
      end)
    rescue
      _e -> []
    end
  end

  defp collect_consumers do
    try do
      ConsumerManager.running_consumers()
      |> Enum.map(fn mod ->
        cfg = mod.__consumer_config__()

        %{
          module: inspect(mod),
          topic: cfg.topic,
          pipeline: to_string(Map.get(cfg, :pipeline, :broadway)),
          concurrency: Map.get(cfg, :concurrency, 1)
        }
      end)
    rescue
      _e -> []
    end
  end

  defp collect_sagas do
    try do
      events = MetricsStore.all()

      %{
        started: Enum.count(events, &(&1.event == :saga_started)),
        completed: Enum.count(events, &(&1.event == :saga_completed)),
        compensated: Enum.count(events, &(&1.event == :saga_compensated)),
        fatal: Enum.count(events, &(&1.event == :saga_failed))
      }
    rescue
      _e -> %{started: 0, completed: 0, compensated: 0, fatal: 0}
    end
  end

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    idx = round(length(sorted) * p / 100) - 1
    idx = max(0, min(idx, length(sorted) - 1))

    sorted
    |> Enum.at(idx, 0)
    |> System.convert_time_unit(:native, :millisecond)
  rescue
    _e -> 0
  end
end
