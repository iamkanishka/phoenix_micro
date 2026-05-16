defmodule PhoenixMicro.Phoenix.HealthPlugTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Middleware.CircuitBreaker.Store, as: CBStore

  # Minimal Plug.Conn builder without a full Phoenix stack
  defp build_conn(path \\ "/") do
    %Plug.Conn{
      request_path: path,
      resp_headers: [],
      state: :unset,
      status: nil,
      resp_body: nil
    }
  end

  setup do
    start_supervised!(MetricsStore)
    start_supervised!(CBStore)
    CBStore.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # init/1
  # ---------------------------------------------------------------------------

  describe "init/1" do
    test "returns opts unchanged" do
      opts = [path: "/health", include_consumers: false]
      assert HealthPlug.init(opts) == opts
    end
  end

  # ---------------------------------------------------------------------------
  # Path matching
  # ---------------------------------------------------------------------------

  describe "path matching" do
    test "serves health when path matches" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      assert conn.status in [200, 503]
    end

    test "passes through when path does not match" do
      conn = build_conn("/other") |> HealthPlug.call(path: "/health")
      # Not halted — conn unchanged
      assert conn.status == nil
    end
  end

  # ---------------------------------------------------------------------------
  # Response body shape
  # ---------------------------------------------------------------------------

  describe "response body" do
    test "returns valid JSON" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      assert is_binary(conn.resp_body)
      assert {:ok, body} = Jason.decode(conn.resp_body)
      assert is_map(body)
    end

    test "body has required top-level keys" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)

      assert Map.has_key?(body, "status")
      assert Map.has_key?(body, "timestamp")
      assert Map.has_key?(body, "transport")
      assert Map.has_key?(body, "consumers")
      assert Map.has_key?(body, "circuit_breakers")
      assert Map.has_key?(body, "pipeline")
    end

    test "status is 'ok' or 'degraded'" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)
      assert body["status"] in ["ok", "degraded"]
    end

    test "timestamp is ISO 8601" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)
      assert {:ok, _h, _b} = DateTime.from_iso8601(body["timestamp"])
    end

    test "transport block has name and status" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)
      transport = body["transport"]
      assert is_binary(transport["active"])
      assert is_binary(transport["status"])
    end

    test "circuit_breakers is a list" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)
      assert is_list(body["circuit_breakers"])
    end

    test "pipeline has running count" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)
      assert is_integer(body["pipeline"]["running"])
    end
  end

  # ---------------------------------------------------------------------------
  # Degraded status when CB is open
  # ---------------------------------------------------------------------------

  describe "status with open circuit breaker" do
    test "returns 503 and status 'degraded' when a breaker is open" do
      CBStore.set_open("test_fuse")

      conn = build_conn("/") |> HealthPlug.call(path: "/")
      {:ok, body} = Jason.decode(conn.resp_body)

      assert conn.status == 503
      assert body["status"] == "degraded"

      # The open breaker appears in the circuit_breakers list
      open_fuses =
        body["circuit_breakers"]
        |> Enum.filter(&(&1["state"] == "open"))
        |> Enum.map(& &1["fuse"])

      assert "test_fuse" in open_fuses
    end
  end

  # ---------------------------------------------------------------------------
  # MetricsStore
  # ---------------------------------------------------------------------------

  describe "MetricsStore" do
    test "starts empty" do
      assert MetricsStore.get(:messages_received) == []
      assert MetricsStore.latest(:messages_received) == nil
    end

    test "records data points via Telemetry events" do
      :telemetry.execute(
        [:phoenix_micro, :message, :received],
        %{count: 1},
        %{topic: "test.topic", transport: :memory}
      )

      Process.sleep(50)

      points = MetricsStore.get(:messages_received)
      refute Enum.empty?(points)

      latest = MetricsStore.latest(:messages_received)
      assert latest != nil
      assert latest.measurements.count == 1
    end

    test "records multiple events and builds ring" do
      for i <- 1..5 do
        :telemetry.execute(
          [:phoenix_micro, :message, :published],
          %{count: 1},
          %{topic: "topic.#{i}"}
        )
      end

      Process.sleep(100)

      points = MetricsStore.get(:messages_published)
      assert Enum.count(points) >= 5
    end

    test "all/0 returns map of all metrics" do
      :telemetry.execute([:phoenix_micro, :rpc, :request], %{count: 1}, %{topic: "math"})
      Process.sleep(50)

      all = MetricsStore.all()
      assert is_map(all)
      assert Map.has_key?(all, :rpc_requests)
    end

    test "sanitise_metadata strips pids and refs" do
      # This exercises the private sanitise_metadata via a Telemetry event
      # with metadata containing a pid (should be removed)
      :telemetry.execute(
        [:phoenix_micro, :message, :received],
        %{count: 1},
        %{topic: "safe.test", consumer: self()}
      )

      Process.sleep(50)
      latest = MetricsStore.latest(:messages_received)
      assert latest != nil
      # The consumer pid should have been sanitised (converted to string or removed)
      refute is_pid(Map.get(latest.metadata, :consumer))
    end
  end

  # ---------------------------------------------------------------------------
  # Content-Type header
  # ---------------------------------------------------------------------------

  describe "response headers" do
    test "sets content-type to application/json" do
      conn = build_conn("/") |> HealthPlug.call(path: "/")
      content_types = for {"content-type", v} <- conn.resp_headers, do: v
      assert Enum.any?(content_types, &String.contains?(&1, "application/json"))
    end
  end
end
