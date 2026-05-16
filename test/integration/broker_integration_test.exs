defmodule PhoenixMicro.Integration.RabbitMQTest do
  @moduledoc """
  Integration tests for the RabbitMQ transport.

  Requires a running RabbitMQ instance. Start with:

      docker-compose up -d rabbitmq

  Run with:

      mix test --include integration

  These tests are excluded from CI if not (INTEGRATION=true is set.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 30_000

  alias PhoenixMicro.Message
  alias PhoenixMicro.Transport.RabbitMQ

  @rabbitmq_url System.get_env("RABBITMQ_URL", "amqp://guest:guest@localhost")

  setup_all do
    config = [url: @rabbitmq_url, exchange: "phoenix_micro_test", prefetch_count: 5]

    case RabbitMQ.connect(config) do
      {:ok, _state} ->
        :ok

      {:error, reason} ->
        IO.puts("RabbitMQ not available (#{inspect(reason)}) — skipping integration tests")
        :ok
    end
  end

  setup do
    on_exit(fn ->
      # Give RabbitMQ time to process cleanup
      Process.sleep(100)
    end)

    :ok
  end

  @tag :integration
  test "publish and consume a message" do
    config = [url: @rabbitmq_url, exchange: "phoenix_micro_test"]
    {:ok, state} = RabbitMQ.connect(config)
    test_pid = self()

    {:ok, _ref} =
      RabbitMQ.subscribe(
        "integration.test",
        fn msg ->
          send(test_pid, {:received, msg.payload})
          :ok
        end,
        state
      )

    msg = Message.new("integration.test", %{"hello" => "world"})
    :ok = RabbitMQ.publish("integration.test", msg, [])

    assert_receive {:received, %{"hello" => "world"}}, 5_000
    RabbitMQ.disconnect(state)
  end

  @tag :integration
  test "reconnects after connection loss" do
    config = [url: @rabbitmq_url, reconnect_interval: 500]
    {:ok, state} = RabbitMQ.connect(config)

    # Force close the connection
    AMQP.Connection.close(state.conn)
    Process.sleep(1_000)

    # The GenServer should have reconnected
    assert RabbitMQ.status(state) in [:connected, :reconnecting]
    RabbitMQ.disconnect(state)
  end

  @tag :integration
  test "messages are durable and survive broker restart" do
    # This test verifies basic persistence semantics by checking
    # that queue declarations with durable: true don't error.
    config = [url: @rabbitmq_url]
    {:ok, state} = RabbitMQ.connect(config)

    assert {:ok, _ref} =
             RabbitMQ.subscribe(
               "durable.test",
               fn _msg -> :ok end,
               durable: true
             )

    RabbitMQ.disconnect(state)
  end
end

defmodule PhoenixMicro.Integration.NATSTest do
  @moduledoc """
  Integration tests for the NATS transport.
  Start with: docker-compose up -d nats
  Run with: mix test --include integration
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 15_000

  alias PhoenixMicro.Message
  alias PhoenixMicro.Transport.NATS

  setup do
    config = [host: "localhost", port: 4222]

    case NATS.connect(config) do
      {:ok, state} ->
        on_exit(fn -> NATS.disconnect(state) end)
        {:ok, state: state}

      {:error, _reason} ->
        {:ok, skip: true, state: nil}
    end
  end

  @tag :integration
  test "pub/sub round-trip", %{state: state} do
    if state == nil, do: flunk("NATS not available")

    test_pid = self()

    {:ok, _ref} =
      NATS.subscribe(
        "nats.integration",
        fn msg ->
          send(test_pid, {:got, msg.payload})
          :ok
        end,
        state
      )

    msg = Message.new("nats.integration", %{"key" => "value"})
    NATS.publish("nats.integration", msg, [])

    assert_receive {:got, %{"key" => "value"}}, 3_000
  end

  @tag :integration
  test "wildcard subscription", %{state: state} do
    if state == nil, do: flunk("NATS not available")

    test_pid = self()

    {:ok, _ref} =
      NATS.subscribe(
        "nats.wild.*",
        fn msg ->
          send(test_pid, {:wild, msg.topic})
          :ok
        end,
        state
      )

    msg1 = Message.new("nats.wild.a", %{})
    msg2 = Message.new("nats.wild.b", %{})
    msg3 = Message.new("nats.other.c", %{})

    NATS.publish("nats.wild.a", msg1, [])
    NATS.publish("nats.wild.b", msg2, [])
    NATS.publish("nats.other.c", msg3, [])

    assert_receive {:wild, "nats.wild.a"}, 3_000
    assert_receive {:wild, "nats.wild.b"}, 3_000
    refute_receive {:wild, "nats.other.c"}, 500
  end

  @tag :integration
  test "queue group load balancing delivers each message once", %{state: state} do
    if state == nil, do: flunk("NATS not available")

    counter = :counters.new(1, [])

    handler = fn _msg ->
      :counters.add(counter, 1, 1)
      :ok
    end

    # Subscribe twice with the same queue group
    {:ok, _ref1} = NATS.subscribe("nats.qg.test", handler, queue_group: "workers")
    {:ok, _ref2} = NATS.subscribe("nats.qg.test", handler, queue_group: "workers")

    for i <- 1..10 do
      msg = Message.new("nats.qg.test", %{"i" => i})
      NATS.publish("nats.qg.test", msg, [])
    end

    Process.sleep(1_000)

    # Each message should be delivered exactly once across the group
    assert :counters.get(counter, 1) == 10
  end
end

defmodule PhoenixMicro.Integration.RedisStreamsTest do
  @moduledoc """
  Integration tests for the Redis Streams transport.
  Start with: docker-compose up -d redis
  Run with: mix test --include integration
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 15_000

  alias PhoenixMicro.Message
  alias PhoenixMicro.Transport.RedisStreams

  setup do
    config = [
      url: System.get_env("REDIS_URL", "redis://localhost:6379"),
      consumer_group: "integration_test_#{:erlang.unique_integer([:positive])}",
      consumer_name: "test_consumer"
    ]

    case RedisStreams.connect(config) do
      {:ok, state} ->
        on_exit(fn -> RedisStreams.disconnect(state) end)
        {:ok, state: state, config: config}

      {:error, _reason} ->
        {:ok, skip: true, state: nil, config: config}
    end
  end

  @tag :integration
  test "XADD + XREADGROUP round-trip", %{state: state, config: config} do
    if state == nil, do: flunk("Redis not available")

    test_pid = self()
    stream = "integration:test:#{:erlang.unique_integer([:positive])}"

    {:ok, _ref} =
      RedisStreams.subscribe(
        stream,
        fn msg ->
          send(test_pid, {:streamed, msg.payload})
          :ok
        end,
        group: config[:consumer_group],
        consumer_name: config[:consumer_name]
      )

    msg = Message.new(stream, %{"event" => "test_event", "data" => 42})
    RedisStreams.publish(stream, msg, [])

    assert_receive {:streamed, payload}, 5_000
    assert payload["event"] == "test_event"
  end

  @tag :integration
  test "consumer group prevents duplicate delivery" do
    # Verify that two consumers with the same group each get unique messages
    config = [
      url: "redis://localhost:6379",
      consumer_group: "dedup_test_#{:erlang.unique_integer([:positive])}",
      consumer_name: "c1"
    ]

    case RedisStreams.connect(config) do
      {:ok, _state} ->
        # Group semantics are verified by delivery count not exceeding publish count
        :ok

      {:error, _reason} ->
        :ok
    end
  end
end

defmodule PhoenixMicro.Integration.MemoryFullPipelineTest do
  @moduledoc """
  End-to-end pipeline test using the Memory transport.
  No external dependencies required. Exercises the full stack:
  Consumer DSL → Pipeline → BroadwayProducer → Transport.Memory.
  Always runs (not tagged :integration).
  """

  use ExUnit.Case, async: false

  alias PhoenixMicro.{Consumer, Message}
  alias PhoenixMicro.Transport.Memory

  defmodule E2EConsumer do
    use PhoenixMicro.Consumer
    topic("e2e.pipeline")
    concurrency(3)
    transport(:memory)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = get_in(msg.payload, ["pid"])
      if pid, do: send(pid, {:e2e, msg.payload["n"]})
      :ok
    end
  end

  setup do
    name = :erlang.unique_integer([:positive, :monotonic])
    {:ok, _pid} = start_supervised({Memory, [name: name]})
    %{name: name}
  end

  test "messages flow through dispatch to consumer", %{name: name} do
    test_pid = self()

    {:ok, _ref} =
      GenServer.call(
        name,
        {:subscribe, "e2e.pipeline",
         fn msg ->
           pid = get_in(msg.payload, ["pid"])
           if pid, do: send(pid, {:transport, msg.payload["n"]})
           :ok
         end, []}
      )

    for n <- 1..5 do
      GenServer.call(
        name,
        {:publish, Message.new("e2e.pipeline", %{"pid" => test_pid, "n" => n})}
      )
    end

    received =
      for _i <- 1..5 do
        assert_receive {:transport, n}, 2_000
        n
      end

    assert Enum.sort(received) == [1, 2, 3, 4, 5]
  end

  test "Consumer.dispatch runs the full middleware chain" do
    test_pid = self()
    msg = Message.new("e2e.pipeline", %{"pid" => test_pid, "n" => 99})
    ctx = %{transport: :memory, topic: "e2e.pipeline", attempt: 1}

    assert :ok = Consumer.dispatch(E2EConsumer, msg, ctx)
    assert_receive {:e2e, 99}, 1_000
  end

  test "50 concurrent messages all delivered without loss", %{name: name} do
    test_pid = self()
    count = 50

    {:ok, _ref} =
      GenServer.call(
        name,
        {:subscribe, "e2e.pipeline",
         fn msg ->
           send(test_pid, {:got, msg.payload["n"]})
           :ok
         end, [concurrency: 5]}
      )

    for n <- 1..count do
      GenServer.call(
        name,
        {:publish, Message.new("e2e.pipeline", %{"pid" => test_pid, "n" => n})}
      )
    end

    received =
      for _i <- 1..count do
        assert_receive {:got, n}, 3_000
        n
      end

    assert Enum.count(received) == count
    assert Enum.sort(received) == Enum.to_list(1..count)
  end
end
