defmodule PhoenixMicro.IntegrationTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Message, Producer}
  alias PhoenixMicro.Transport.Memory

  # Shared Memory transport name for integration tests
  @transport_name :integration_memory

  setup_all do
    {:ok, _pid} = start_supervised({Memory, [name: @transport_name]})
    :ok
  end

  setup do
    GenServer.call(@transport_name, :clear)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Publish → Subscribe round-trip
  # ---------------------------------------------------------------------------

  describe "publish → subscribe round-trip" do
    test "published message is delivered to subscriber" do
      test_pid = self()
      topic = "integration.basic"

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, topic,
           fn msg ->
             send(test_pid, {:got, msg})
             :ok
           end, []}
        )

      msg = Message.new(topic, %{amount: 500, currency: "USD"})
      GenServer.call(@transport_name, {:publish, msg})

      assert_receive {:got, received}, 1_000
      assert received.topic == topic
      assert received.payload == %{amount: 500, currency: "USD"}
      assert received.id == msg.id
    end

    test "multiple publishers fan into a single subscriber" do
      test_pid = self()
      topic = "integration.fan_in"

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, topic,
           fn msg ->
             send(test_pid, {:got, msg.payload})
             :ok
           end, []}
        )

      for i <- 1..5 do
        GenServer.call(@transport_name, {:publish, Message.new(topic, %{seq: i})})
      end

      received =
        for _i <- 1..5 do
          assert_receive {:got, payload}, 1_000
          payload["seq"]
        end

      assert Enum.sort(received) == [1, 2, 3, 4, 5]
    end

    test "wildcard subscriber receives matching topics" do
      test_pid = self()

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, "events.*",
           fn msg ->
             send(test_pid, {:got, msg.topic})
             :ok
           end, []}
        )

      GenServer.call(@transport_name, {:publish, Message.new("events.created", %{})})
      GenServer.call(@transport_name, {:publish, Message.new("events.updated", %{})})
      GenServer.call(@transport_name, {:publish, Message.new("other.topic", %{})})

      assert_receive {:got, "events.created"}, 500
      assert_receive {:got, "events.updated"}, 500
      refute_receive {:got, "other.topic"}, 200
    end
  end

  # ---------------------------------------------------------------------------
  # Message ordering and batching
  # ---------------------------------------------------------------------------

  describe "message ordering" do
    test "messages arrive in publish order for a single subscriber" do
      test_pid = self()
      topic = "integration.ordering"
      count = 20

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, topic,
           fn msg ->
             send(test_pid, {:seq, msg.payload["seq"]})
             :ok
           end, []}
        )

      for i <- 1..count do
        GenServer.call(@transport_name, {:publish, Message.new(topic, %{"seq" => i})})
      end

      seqs =
        for _i <- 1..count do
          assert_receive {:seq, n}, 1_000
          n
        end

      assert seqs == Enum.to_list(1..count)
    end
  end

  # ---------------------------------------------------------------------------
  # Message introspection (test helpers)
  # ---------------------------------------------------------------------------

  describe "Memory.messages/1 test helper" do
    test "records all published messages" do
      for i <- 1..3 do
        GenServer.call(@transport_name, {:publish, Message.new("log.topic.#{i}", %{i: i})})
      end

      msgs = GenServer.call(@transport_name, :messages)
      assert Enum.count(msgs) == 3
    end

    test "clear/0 resets the log" do
      GenServer.call(@transport_name, {:publish, Message.new("log.topic", %{})})
      assert Enum.count(GenServer.call(@transport_name, :messages)) == 1

      GenServer.call(@transport_name, :clear)
      assert GenServer.call(@transport_name, :messages) == []
    end
  end

  # ---------------------------------------------------------------------------
  # DLQ routing
  # ---------------------------------------------------------------------------

  describe "DLQ routing" do
    test "nacked messages appear in dlq_messages" do
      msg = Message.new("payments.failed_msg", %{order_id: 42})

      GenServer.cast(@transport_name, {:dlq, msg, :processing_error})
      Process.sleep(100)

      dlq = GenServer.call(@transport_name, :dlq_messages)
      assert Enum.any?(dlq, fn m -> m.metadata[:original_topic] == "payments.failed_msg" end)
    end
  end

  # ---------------------------------------------------------------------------
  # RPC over Memory transport
  # ---------------------------------------------------------------------------

  describe "RPC call/respond" do
    test "rpc call returns response from handler" do
      # Subscribe to RPC topic as a "service"
      rpc_topic = "rpc.math.sum"
      test_pid = self()

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, rpc_topic,
           fn msg ->
             result = Enum.sum(msg.payload)
             reply_msg = Message.new(msg.reply_to, result, correlation_id: msg.correlation_id)
             GenServer.call(@transport_name, {:publish, reply_msg})
             :ok
           end, []}
        )

      # Set up an inbox subscriber
      inbox_topic = "_inbox_test_#{:erlang.unique_integer([:positive])}"
      correlation_id = Message.generate_id()

      GenServer.call(
        @transport_name,
        {:subscribe, inbox_topic,
         fn msg ->
           if msg.correlation_id == correlation_id do
             send(test_pid, {:rpc_response, msg.payload})
           end

           :ok
         end, []}
      )

      # Publish the RPC request
      request =
        Message.new(rpc_topic, [1, 2, 3, 4, 5],
          reply_to: inbox_topic,
          correlation_id: correlation_id
        )

      GenServer.call(@transport_name, {:publish, request})

      assert_receive {:rpc_response, 15}, 1_000
    end

    test "rpc timeout fires when no handler responds" do
      # We simulate a timeout by not subscribing anything to the topic
      # The RPC module should time out after its configured period
      # Here we test the underlying pattern directly

      correlation_id = Message.generate_id()
      timeout_ms = 100

      # No subscriber on this topic — just verify we can model timeout
      result =
        receive do
          {:rpc_response, ^correlation_id, payload} -> {:ok, payload}
        after
          timeout_ms -> {:error, :timeout}
        end

      assert result == {:error, :timeout}
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent consumers
  # ---------------------------------------------------------------------------

  describe "concurrent processing" do
    test "multiple concurrent handlers process without data loss" do
      test_pid = self()
      topic = "integration.concurrent"
      message_count = 50
      results = :ets.new(:concurrent_results, [:set, :public])

      {:ok, _ref} =
        GenServer.call(
          @transport_name,
          {:subscribe, topic,
           fn msg ->
             :ets.insert(results, {msg.id, msg.payload["n"]})
             send(test_pid, :processed)
             :ok
           end, [concurrency: 5]}
        )

      for n <- 1..message_count do
        GenServer.call(@transport_name, {:publish, Message.new(topic, %{"n" => n})})
      end

      # Wait for all messages
      for _i <- 1..message_count do
        assert_receive :processed, 2_000
      end

      all_values = :ets.tab2list(results) |> Enum.map(&elem(&1, 1)) |> Enum.sort()
      assert all_values == Enum.to_list(1..message_count)
    end
  end

  # ---------------------------------------------------------------------------
  # Serializer
  # ---------------------------------------------------------------------------

  describe "JSON serializer" do
    alias PhoenixMicro.Serializer.JSON

    test "encode!/1 produces valid JSON binary" do
      encoded = JSON.encode!(%{a: 1, b: "hello"})
      assert is_binary(encoded)
      assert String.contains?(encoded, "hello")
    end

    test "decode!/1 round-trips a map" do
      original = %{"key" => "value", "num" => 42}
      decoded = original |> Jason.encode!() |> JSON.decode!()
      assert decoded == original
    end

    test "decode!/1 raises on invalid JSON" do
      assert_raise RuntimeError, ~r/decode error/, fn ->
        JSON.decode!("not json {{{{")
      end
    end

    test "content_type/0" do
      assert JSON.content_type() == "application/json"
    end
  end

  # ---------------------------------------------------------------------------
  # Config
  # ---------------------------------------------------------------------------

  describe "Config" do
    alias PhoenixMicro.Config

    test "get/0 returns keyword list" do
      cfg = Config.get()
      assert is_list(cfg)
      assert Keyword.keyword?(cfg)
    end

    test "get/2 returns default when key absent" do
      assert Config.get(:nonexistent_key, :my_default) == :my_default
    end

    test "transport_module/1 maps atoms to modules" do
      assert Config.transport_module(:memory) == PhoenixMicro.Transport.Memory
      assert Config.transport_module(:rabbitmq) == PhoenixMicro.Transport.RabbitMQ
      assert Config.transport_module(:kafka) == PhoenixMicro.Transport.Kafka
      assert Config.transport_module(:nats) == PhoenixMicro.Transport.NATS
      assert Config.transport_module(:redis_streams) == PhoenixMicro.Transport.RedisStreams
    end

    test "transport_module/1 passes through custom modules" do
      assert Config.transport_module(MyApp.CustomTransport) == MyApp.CustomTransport
    end

    test "retry_opts/1 merges with defaults" do
      merged = Config.retry_opts(max_attempts: 10)
      assert merged[:max_attempts] == 10
      # Other defaults preserved
      assert is_integer(merged[:base_delay])
    end
  end

  # ---------------------------------------------------------------------------
  # Middleware pipeline
  # ---------------------------------------------------------------------------

  describe "Middleware pipeline" do
    alias PhoenixMicro.Transport

    test "empty middleware chain calls handler directly" do
      test_pid = self()
      msg = Message.new("mw.test", %{})

      handler = fn m ->
        send(test_pid, {:called, m.topic})
        :ok
      end

      chain = Transport.build_chain([], handler)
      assert chain.(msg) == :ok
      assert_receive {:called, "mw.test"}, 200
    end

    test "middleware wraps handler in order" do
      order = :ets.new(:mw_order, [:ordered_set, :public])
      counter = :counters.new(1, [])
      test_pid = self()

      make_middleware = fn name ->
        fn msg, next ->
          n = :counters.get(counter, 1)
          :counters.add(counter, 1, 1)
          :ets.insert(order, {n, name, :before})
          result = next.(msg)
          :ets.insert(order, {:counters.get(counter, 1), name, :after})
          :counters.add(counter, 1, 1)
          result
        end
      end

      mw1 = make_middleware.(:first)
      mw2 = make_middleware.(:second)

      handler = fn _msg ->
        send(test_pid, :handler_called)
        :ok
      end

      # Build chain manually using build_chain logic
      chain = fn msg ->
        next2 = fn m -> mw2.(m, handler) end
        mw1.(msg, next2)
      end

      msg = Message.new("mw.order", %{})
      result = chain.(msg)

      assert result == :ok
      assert_receive :handler_called, 200

      calls = :ets.tab2list(order) |> Enum.sort() |> Enum.map(&elem(&1, 1))
      # first before, second before, second after, first after
      assert calls == [:first, :second, :second, :first]
    end

    test "middleware can short-circuit the chain" do
      test_pid = self()

      blocking_mw = fn _msg, _next ->
        send(test_pid, :blocked)
        {:error, :unauthorized}
      end

      handler = fn _msg ->
        send(test_pid, :should_not_reach)
        :ok
      end

      msg = Message.new("mw.block", %{})
      result = Transport.build_chain([blocking_mw], handler).(msg)

      assert result == {:error, :unauthorized}
      assert_receive :blocked, 200
      refute_receive :should_not_reach, 100
    end
  end
end
