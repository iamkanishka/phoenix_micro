defmodule PhoenixMicro.Phoenix.HealthPlug do
  @moduledoc """
  A Plug that exposes a JSON health endpoint for `phoenix_micro`.

  Mount in your Phoenix router:

      forward "/health/microservices", PhoenixMicro.Phoenix.HealthPlug

  Requires `{:plug, "~> 1.15"}` in your application's deps.

  Returns 200 OK when healthy, 503 when degraded.
  """

  @plug_available Code.ensure_loaded?(Plug.Conn)

  require Logger

  alias PhoenixMicro.{Config, Supervisor.ConsumerManager}
  alias PhoenixMicro.Middleware.CircuitBreaker.Store, as: CBStore

  if @plug_available do
    @behaviour Plug

    @impl Plug
    def init(opts), do: opts

    @impl Plug
    def call(conn, opts) do
      include_consumers = Keyword.get(opts, :include_consumers, true)
      include_cbs = Keyword.get(opts, :include_circuit_breakers, true)
      health = build_health(include_consumers, include_cbs)
      status_code = if health.status == "ok", do: 200, else: 503
      body = Jason.encode!(health)

      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.send_resp(status_code, body)
      |> Plug.Conn.halt()
    end
  else
    def init(opts), do: opts

    def call(conn, _opts) do
      Logger.warning(
        "[PhoenixMicro.Phoenix.HealthPlug] Plug is not installed. " <>
          "Add {:plug, \"~> 1.15\"} to your application's deps."
      )

      conn
    end
  end

  @spec build_health(boolean(), boolean()) :: map()
  def build_health(include_consumers \\ true, include_cbs \\ true) do
    transport = transport_status()
    consumers = if include_consumers, do: consumer_status(), else: []
    cbs = if include_cbs, do: cb_status(), else: []

    overall =
      cond do
        transport.status != "connected" -> "degraded"
        Enum.any?(cbs, &(&1.state == "open")) -> "degraded"
        true -> "ok"
      end

    %{
      status: overall,
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
      transport: transport,
      consumers: consumers,
      circuit_breakers: cbs,
      pipeline: pipeline_status()
    }
  end

  defp transport_status do
    mod = Config.active_transport()
    name = mod |> Module.split() |> List.last() |> String.downcase()

    status =
      try do
        case mod.status(%{}) do
          :connected -> "connected"
          :disconnected -> "disconnected"
          _other -> "unknown"
        end
      rescue
        _e -> "unknown"
      end

    %{active: name, status: status}
  end

  defp consumer_status do
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

  defp cb_status do
    try do
      CBStore.all_states()
      |> Enum.map(fn
        {fuse, :closed, _ts} -> %{fuse: fuse, state: "closed"}
        {fuse, :open, opened_at} -> %{fuse: fuse, state: "open", opened_at_ms: opened_at}
        {fuse, :half_open, _ts} -> %{fuse: fuse, state: "half_open"}
      end)
    rescue
      _e -> []
    end
  end

  defp pipeline_status do
    try do
      %{running: ConsumerManager.running_consumers() |> length()}
    rescue
      _err -> %{running: 0}
    end
  end
end
