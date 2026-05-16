defmodule PhoenixMicro.TelemetryTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Telemetry

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp attach(events, test_pid) do
    ref = make_ref()

    :telemetry.attach_many(
      inspect(ref),
      events,
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(inspect(ref)) end)
    ref
  end

  # ---------------------------------------------------------------------------
  # Event emission
  # ---------------------------------------------------------------------------

  describe "message_received/2" do
    test "emits the correct event with count=1" do
      attach([[:phoenix_micro, :message, :received]], self())

      Telemetry.message_received("payments.created", %{transport: :memory})

      assert_receive {:telemetry_event, [:phoenix_micro, :message, :received], %{count: 1},
                      %{topic: "payments.created", transport: :memory}},
                     500
    end

    test "works with empty metadata" do
      attach([[:phoenix_micro, :message, :received]], self())
      Telemetry.message_received("t")
      assert_receive {:telemetry_event, [:phoenix_micro, :message, :received], _, _}, 300
    end
  end

  describe "message_processed/2" do
    test "emits duration measurement" do
      attach([[:phoenix_micro, :message, :processed]], self())

      Telemetry.message_processed("orders.placed", %{duration: 12_345, transport: :kafka})

      assert_receive {:telemetry_event, [:phoenix_micro, :message, :processed],
                      %{duration: 12_345}, meta},
                     500

      assert meta.topic == "orders.placed"
      assert meta.transport == :kafka
    end

    test "defaults duration to 0 when not provided" do
      attach([[:phoenix_micro, :message, :processed]], self())
      Telemetry.message_processed("t")

      assert_receive {:telemetry_event, _, %{duration: 0}, _}, 300
    end
  end

  describe "message_failed/2" do
    test "emits failed event with reason" do
      attach([[:phoenix_micro, :message, :failed]], self())

      Telemetry.message_failed("orders.failed", %{reason: :timeout, final: true})

      assert_receive {:telemetry_event, [:phoenix_micro, :message, :failed], %{count: 1},
                      %{topic: "orders.failed", reason: :timeout, final: true}},
                     500
    end
  end

  describe "message_published/2" do
    test "emits published event" do
      attach([[:phoenix_micro, :message, :published]], self())

      Telemetry.message_published("payments.created", %{transport: :rabbitmq, batched: false})

      assert_receive {:telemetry_event, [:phoenix_micro, :message, :published], %{count: 1},
                      %{topic: "payments.created", transport: :rabbitmq}},
                     500
    end
  end

  describe "rpc_request/2" do
    test "emits rpc request event" do
      attach([[:phoenix_micro, :rpc, :request]], self())

      Telemetry.rpc_request("math.sum", %{correlation_id: "abc123"})

      assert_receive {:telemetry_event, [:phoenix_micro, :rpc, :request], %{count: 1},
                      %{topic: "math.sum", correlation_id: "abc123"}},
                     500
    end
  end

  describe "rpc_response/2" do
    test "emits rpc response with duration" do
      attach([[:phoenix_micro, :rpc, :response]], self())

      Telemetry.rpc_response("math.sum", %{duration: 987, correlation_id: "abc"})

      assert_receive {:telemetry_event, [:phoenix_micro, :rpc, :response], %{duration: 987},
                      _meta},
                     500
    end
  end

  describe "rpc_timeout/2" do
    test "emits timeout event" do
      attach([[:phoenix_micro, :rpc, :timeout]], self())

      Telemetry.rpc_timeout("slow.service", %{correlation_id: "xyz"})

      assert_receive {:telemetry_event, [:phoenix_micro, :rpc, :timeout], %{count: 1},
                      %{topic: "slow.service"}},
                     500
    end
  end

  describe "transport_connected/1 and transport_disconnected/2" do
    test "emits transport connected event" do
      attach([[:phoenix_micro, :transport, :connected]], self())

      Telemetry.transport_connected(:rabbitmq)

      assert_receive {:telemetry_event, [:phoenix_micro, :transport, :connected], %{count: 1},
                      %{transport: :rabbitmq}},
                     500
    end

    test "emits transport disconnected event with reason" do
      attach([[:phoenix_micro, :transport, :disconnected]], self())

      Telemetry.transport_disconnected(:kafka, :connection_refused)

      assert_receive {:telemetry_event, [:phoenix_micro, :transport, :disconnected], %{count: 1},
                      %{transport: :kafka, reason: :connection_refused}},
                     500
    end
  end

  # ---------------------------------------------------------------------------
  # attach_default_logger/1
  # ---------------------------------------------------------------------------

  describe "attach_default_logger/1" do
    test "attaches without error and returns :ok" do
      # Detach first in case already attached from a previous test
      :telemetry.detach("phoenix_micro_default_logger")

      result = Telemetry.attach_default_logger(level: :debug)
      assert result == :ok

      # Clean up
      :telemetry.detach("phoenix_micro_default_logger")
    end
  end

  # ---------------------------------------------------------------------------
  # metrics/0
  # ---------------------------------------------------------------------------

  describe "metrics/0" do
    test "returns a non-empty list of metric structs" do
      metrics = Telemetry.metrics()
      assert is_list(metrics)
      assert metrics != []
    end

    test "all metrics have expected event names" do
      metrics = Telemetry.metrics()

      event_names =
        metrics
        |> Enum.map(fn m -> m.event_name end)
        |> Enum.uniq()

      # All should be phoenix_micro events
      assert Enum.all?(event_names, fn name ->
               List.first(name) == :phoenix_micro
             end)
    end
  end
end
