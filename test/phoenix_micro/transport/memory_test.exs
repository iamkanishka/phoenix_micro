defmodule PhoenixMicro.Transport.MemoryTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Message, Transport.Memory}

  setup do
    # Start a fresh named Memory server for each test using a unique name
    name = :erlang.unique_integer([:positive, :monotonic])
    {:ok, pid} = start_supervised({Memory, [name: name]})
    %{transport: pid, name: name}
  end

  describe "connect/1" do
    test "returns :ok state" do
      assert {:ok, state} = Memory.connect([])
      assert state.subscriptions == %{}
    end
  end

  describe "status/1" do
    test "returns :connected" do
      {:ok, state} = Memory.connect([])
      assert Memory.status(state) == :connected
    end
  end

  describe "publish/3 + subscribe/3" do
    test "delivers message to subscriber", %{name: name} do
      test_pid = self()

      {:ok, _ref} =
        GenServer.call(
          name,
          {:subscribe, "test.topic",
           fn msg ->
             send(test_pid, {:received, msg})
             :ok
           end, []}
        )

      msg = Message.new("test.topic", %{value: 42})
      GenServer.call(name, {:publish, msg})

      assert_receive {:received, received_msg}, 500
      assert received_msg.topic == "test.topic"
      assert received_msg.payload == %{value: 42}
    end

    test "delivers to multiple subscribers for the same topic", %{name: name} do
      test_pid = self()

      for i <- 1..3 do
        GenServer.call(
          name,
          {:subscribe, "test.multi",
           fn msg ->
             send(test_pid, {:received, i, msg})
             :ok
           end, []}
        )
      end

      msg = Message.new("test.multi", "hello")
      GenServer.call(name, {:publish, msg})

      for i <- 1..3 do
        assert_receive {:received, ^i, _msg}, 500
      end
    end

    test "does not deliver to non-matching subscribers", %{name: name} do
      test_pid = self()

      GenServer.call(
        name,
        {:subscribe, "payments.created",
         fn _msg ->
           send(test_pid, :should_not_receive)
           :ok
         end, []}
      )

      msg = Message.new("orders.created", %{})
      GenServer.call(name, {:publish, msg})

      refute_receive :should_not_receive, 200
    end
  end

  describe "wildcard topic matching" do
    test "* matches a single segment" do
      assert Memory.topic_matches?("payments.*", "payments.created")
      assert Memory.topic_matches?("payments.*", "payments.updated")
      refute Memory.topic_matches?("payments.*", "payments.created.v2")
      refute Memory.topic_matches?("payments.*", "orders.created")
    end

    test "> matches multiple segments" do
      assert Memory.topic_matches?("payments.>", "payments.created")
      assert Memory.topic_matches?("payments.>", "payments.created.v2")
      assert Memory.topic_matches?("payments.>", "payments.a.b.c.d")
      refute Memory.topic_matches?("payments.>", "orders.created")
    end

    test "exact match works" do
      assert Memory.topic_matches?("payments.created", "payments.created")
      refute Memory.topic_matches?("payments.created", "payments.updated")
    end

    test "nested wildcard" do
      assert Memory.topic_matches?("a.*.c", "a.b.c")
      refute Memory.topic_matches?("a.*.c", "a.b.d")
      refute Memory.topic_matches?("a.*.c", "a.b.b.c")
    end
  end

  describe "messages/1" do
    test "returns all published messages", %{name: name} do
      msg1 = Message.new("t1", "a")
      msg2 = Message.new("t2", "b")

      GenServer.call(name, {:publish, msg1})
      GenServer.call(name, {:publish, msg2})

      msgs = GenServer.call(name, :messages)
      assert Enum.count(msgs) == 2
      topics = Enum.map(msgs, & &1.topic)
      assert "t1" in topics
      assert "t2" in topics
    end
  end

  describe "clear/1" do
    test "clears the message log", %{name: name} do
      msg = Message.new("test", "x")
      GenServer.call(name, {:publish, msg})
      assert Enum.count(GenServer.call(name, :messages)) == 1

      GenServer.call(name, :clear)
      assert GenServer.call(name, :messages) == []
    end
  end

  describe "unsubscribe/2" do
    test "stops delivering after unsubscribe", %{name: name} do
      test_pid = self()
      count = :counters.new(1, [])

      {:ok, ref} =
        GenServer.call(
          name,
          {:subscribe, "test.unsub",
           fn _msg ->
             :counters.add(count, 1, 1)
             send(test_pid, :delivered)
             :ok
           end, []}
        )

      # First message gets delivered
      GenServer.call(name, {:publish, Message.new("test.unsub", "first")})
      assert_receive :delivered, 500

      # Unsubscribe
      GenServer.call(name, {:unsubscribe, ref})

      # Second message should NOT be delivered
      GenServer.call(name, {:publish, Message.new("test.unsub", "second")})
      refute_receive :delivered, 200

      assert :counters.get(count, 1) == 1
    end
  end

  describe "nack/3 → DLQ" do
    test "nacked messages appear in dlq_messages", %{name: name} do
      msg = Message.new("test.dlq", %{id: 99})
      GenServer.cast(name, {:dlq, msg, :test_reason})

      # Give the cast time to process
      Process.sleep(50)

      dlq = GenServer.call(name, :dlq_messages)
      refute Enum.empty?(dlq)
      assert hd(dlq).metadata[:original_topic] == "test.dlq"
    end
  end
end
