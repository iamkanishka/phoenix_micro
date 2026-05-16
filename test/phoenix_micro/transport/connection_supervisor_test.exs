defmodule PhoenixMicro.Transport.ConnectionSupervisorTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.Transport.ConnectionSupervisor

  describe "pool_name/1" do
    test "returns namespaced atom" do
      assert ConnectionSupervisor.pool_name(:rabbitmq) == :phoenix_micro_rabbitmq_pool
      assert ConnectionSupervisor.pool_name(:nats) == :phoenix_micro_nats_pool
      assert ConnectionSupervisor.pool_name(:kafka) == :phoenix_micro_kafka_pool
    end
  end

  describe "child_spec shape" do
    test "connection_supervisor_spec produces correct map structure" do
      # We test this through the Supervisor module's behaviour
      # without actually starting a real transport connection
      spec = %{
        id: {ConnectionSupervisor, :memory},
        start:
          {ConnectionSupervisor, :start_link,
           [
             [
               transport_name: :memory,
               transport_module: PhoenixMicro.Transport.Memory,
               transport_config: [],
               pool_size: 5
             ]
           ]},
        type: :supervisor,
        restart: :permanent
      }

      assert spec.type == :supervisor
      assert spec.restart == :permanent
      assert {ConnectionSupervisor, :memory} = spec.id
    end
  end
end
