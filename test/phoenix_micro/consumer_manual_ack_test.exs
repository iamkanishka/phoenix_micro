defmodule PhoenixMicro.Consumer.ManualAckTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Consumer, Message}
  alias PhoenixMicro.Transport.Memory

  setup do
    name = :erlang.unique_integer([:positive, :monotonic])
    {:ok, _pid} = start_supervised({Memory, [name: name]})
    %{name: name}
  end

  describe "Consumer.ack/2" do
    test "acks a message via the transport in context" do
      msg = Message.new("test.manual", %{})

      context = %{
        transport: :memory,
        topic: "test.manual",
        attempt: 1,
        transport_mod: PhoenixMicro.Transport.Memory,
        message: msg
      }

      # ack should not raise and returns :ok
      assert :ok = Consumer.ack(msg, context)
    end

    test "works without transport_mod in context (fallback)" do
      msg = Message.new("test.fallback", %{})
      context = %{transport: :memory, topic: "test.fallback", attempt: 1}

      # Falls back to configured transport
      assert :ok = Consumer.ack(msg, context)
    end
  end

  describe "Consumer.nack/3" do
    test "nacks a message with a reason" do
      msg = Message.new("test.nack", %{})

      context = %{
        transport: :memory,
        topic: "test.nack",
        attempt: 1,
        transport_mod: PhoenixMicro.Transport.Memory,
        message: msg
      }

      assert :ok = Consumer.nack(msg, context, :test_reason)
    end

    test "uses default reason when not provided" do
      msg = Message.new("test.nack.default", %{})

      context = %{
        transport: :memory,
        topic: "test.nack.default",
        attempt: 1,
        transport_mod: PhoenixMicro.Transport.Memory,
        message: msg
      }

      assert :ok = Consumer.nack(msg, context)
    end
  end

  describe "context includes transport_mod and message" do
    defmodule ContextCheckConsumer do
      use PhoenixMicro.Consumer
      topic("context.check")
      transport(:memory)

      @impl PhoenixMicro.Consumer
      def handle(%Message{} = message, context) do
        pid = get_in(message.payload, ["test_pid"])

        send(pid, {
          :context_fields,
          Map.has_key?(context, :transport_mod),
          Map.has_key?(context, :message),
          Map.has_key?(context, :attempt),
          Map.has_key?(context, :topic)
        })

        :ok
      end
    end

    test "dispatch/3 provides full context including transport_mod and message" do
      test_pid = self()
      msg = Message.new("context.check", %{"test_pid" => test_pid})

      ctx = %{
        transport: :memory,
        topic: "context.check",
        attempt: 1,
        transport_mod: PhoenixMicro.Transport.Memory,
        message: msg
      }

      Consumer.dispatch(ContextCheckConsumer, msg, ctx)

      assert_receive {:context_fields, has_transport_mod, has_message, has_attempt, has_topic},
                     500

      assert has_transport_mod
      assert has_message
      assert has_attempt
      assert has_topic
    end
  end

  describe "consumer can call manual ack inside handle/2" do
    defmodule ManualAckConsumer do
      use PhoenixMicro.Consumer
      topic("manual.ack")
      transport(:memory)

      @impl PhoenixMicro.Consumer
      def handle(%Message{} = message, context) do
        # Manually ack before returning
        PhoenixMicro.Consumer.ack(message, context)
        pid = get_in(message.payload, ["test_pid"])
        send(pid, :manually_acked)
        :ok
      end
    end

    test "can manually ack from within handle/2" do
      test_pid = self()
      msg = Message.new("manual.ack", %{"test_pid" => test_pid})

      ctx = %{
        transport: :memory,
        topic: "manual.ack",
        attempt: 1,
        transport_mod: PhoenixMicro.Transport.Memory,
        message: msg
      }

      assert :ok = Consumer.dispatch(ManualAckConsumer, msg, ctx)
      assert_receive :manually_acked, 500
    end
  end
end
