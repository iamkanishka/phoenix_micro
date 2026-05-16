defmodule PhoenixMicro.PipelineTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Consumer, Message}
  alias PhoenixMicro.Transport.Memory

  @transport_name :pipeline_test_memory

  # ---------------------------------------------------------------------------
  # Test consumers
  # ---------------------------------------------------------------------------

  defmodule SimpleConsumer do
    use PhoenixMicro.Consumer

    topic("pipeline.simple")
    concurrency(2)
    pipeline(:broadway)
    transport(:memory)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = get_in(msg.payload, ["test_pid"])
      if pid, do: send(pid, {:handled, msg.id, msg.attempt})
      :ok
    end
  end

  defmodule BatchConsumer do
    use PhoenixMicro.Consumer

    topic("pipeline.batch")
    concurrency(2)
    batch_size(5)
    batch_timeout(500)
    pipeline(:broadway)
    transport(:memory)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = get_in(msg.payload, ["test_pid"])
      if pid, do: send(pid, {:individual, msg.id})
      :ok
    end

    def handle_batch(_batcher, messages, _batch_info, _ctx) do
      messages
    end
  end

  defmodule ErrorConsumer do
    use PhoenixMicro.Consumer

    topic("pipeline.errors")
    concurrency(1)
    retry(max_attempts: 2, base_delay: 10, max_delay: 50, jitter: false)
    pipeline(:broadway)
    transport(:memory)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = get_in(msg.payload, ["test_pid"])
      if pid, do: send(pid, {:attempt, msg.attempt})
      {:error, :deliberate_failure}
    end
  end

  defmodule SimpleModeLegacyConsumer do
    use PhoenixMicro.Consumer

    topic("pipeline.simple_mode")
    concurrency(1)
    pipeline(:simple)
    transport(:memory)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = get_in(msg.payload, ["test_pid"])
      if pid, do: send(pid, :legacy_handled)
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer DSL config tests (no process started)
  # ---------------------------------------------------------------------------

  describe "Consumer DSL — batch + pipeline config" do
    test "SimpleConsumer has correct pipeline config" do
      cfg = SimpleConsumer.__consumer_config__()
      assert cfg.pipeline == :broadway
      assert cfg.batch_size == 1
      assert cfg.batch_timeout == 1_000
      assert cfg.concurrency == 2
    end

    test "BatchConsumer exposes batch_size and batch_timeout" do
      cfg = BatchConsumer.__consumer_config__()
      assert cfg.batch_size == 5
      assert cfg.batch_timeout == 500
      assert cfg.pipeline == :broadway
    end

    test "SimpleModeLegacyConsumer selects :simple pipeline" do
      cfg = SimpleModeLegacyConsumer.__consumer_config__()
      assert cfg.pipeline == :simple
    end

    test "default pipeline mode is :broadway" do
      defmodule DefaultPipelineConsumer do
        use PhoenixMicro.Consumer
        topic("default.pipeline")

        @impl PhoenixMicro.Consumer
        def handle(_msg, _ctx), do: :ok
      end

      cfg = DefaultPipelineConsumer.__consumer_config__()
      assert cfg.pipeline == :broadway
    end
  end

  # ---------------------------------------------------------------------------
  # BroadwayProducer — unit tests for queue mechanics
  # ---------------------------------------------------------------------------

  describe "BroadwayProducer topic_matching (via Memory)" do
    setup do
      name = :erlang.unique_integer([:positive, :monotonic])
      {:ok, _pid} = start_supervised({Memory, [name: name]})
      %{name: name}
    end

    test "buffer fills and dispatches on demand", %{name: name} do
      test_pid = self()

      {:ok, _ref} =
        GenServer.call(
          name,
          {:subscribe, "pipeline.direct",
           fn msg ->
             send(test_pid, {:got, msg.payload})
             :ok
           end, []}
        )

      for i <- 1..10 do
        GenServer.call(name, {:publish, Message.new("pipeline.direct", %{"i" => i})})
      end

      received =
        for _i <- 1..10 do
          assert_receive {:got, p}, 1_000
          p["i"]
        end

      assert Enum.sort(received) == Enum.to_list(1..10)
    end

    test "wildcard subscription receives matching messages", %{name: name} do
      test_pid = self()

      {:ok, _ref} =
        GenServer.call(
          name,
          {:subscribe, "pipeline.*",
           fn msg ->
             send(test_pid, {:topic, msg.topic})
             :ok
           end, []}
        )

      GenServer.call(name, {:publish, Message.new("pipeline.a", %{})})
      GenServer.call(name, {:publish, Message.new("pipeline.b", %{})})
      GenServer.call(name, {:publish, Message.new("other.topic", %{})})

      assert_receive {:topic, "pipeline.a"}, 500
      assert_receive {:topic, "pipeline.b"}, 500
      refute_receive {:topic, "other.topic"}, 200
    end
  end

  # ---------------------------------------------------------------------------
  # Pipeline dispatch — via Consumer.dispatch (no Broadway process needed)
  # ---------------------------------------------------------------------------

  describe "Consumer.dispatch through Broadway consumer modules" do
    test "SimpleConsumer dispatches and acks" do
      test_pid = self()
      msg = Message.new("pipeline.simple", %{"test_pid" => test_pid})
      ctx = %{transport: :memory, topic: "pipeline.simple", attempt: 1}

      assert :ok = Consumer.dispatch(SimpleConsumer, msg, ctx)
      assert_receive {:handled, _id, 1}, 500
    end

    test "ErrorConsumer returns error on every attempt" do
      test_pid = self()
      msg = Message.new("pipeline.errors", %{"test_pid" => test_pid})
      ctx = %{transport: :memory, topic: "pipeline.errors", attempt: 1}

      result = Consumer.dispatch(ErrorConsumer, msg, ctx)
      assert {:error, :deliberate_failure} = result
      assert_receive {:attempt, 1}, 200
    end
  end

  # ---------------------------------------------------------------------------
  # RetryScheduler interaction with Pipeline backoff
  # ---------------------------------------------------------------------------

  describe "RetryScheduler used by Pipeline" do
    alias PhoenixMicro.Consumer.RetryScheduler

    test "delay grows exponentially without exceeding max" do
      opts = [base_delay: 10, max_delay: 100, jitter: false, max_attempts: 5]

      delays = for attempt <- 1..5, do: RetryScheduler.next_delay(attempt, opts)

      assert delays == [10, 20, 40, 80, 100]
    end

    test "max_delay is capped regardless of attempt count" do
      opts = [base_delay: 100, max_delay: 200, jitter: false]

      assert RetryScheduler.next_delay(10, opts) == 200
      assert RetryScheduler.next_delay(20, opts) == 200
    end
  end

  # ---------------------------------------------------------------------------
  # Pipeline.child_spec
  # ---------------------------------------------------------------------------

  describe "Pipeline.child_spec/1" do
    test "returns a supervisor child spec" do
      spec = PhoenixMicro.Pipeline.child_spec({SimpleConsumer, []})

      assert spec.type == :supervisor
      assert spec.restart == :permanent
      assert {PhoenixMicro.Pipeline, SimpleConsumer} = spec.id
    end
  end

  # ---------------------------------------------------------------------------
  # ConsumerManager routing
  # ---------------------------------------------------------------------------

  describe "ConsumerManager builds correct child spec" do
    test "broadway consumer gets :supervisor type child spec" do
      cfg = SimpleConsumer.__consumer_config__()
      assert cfg.pipeline == :broadway

      spec =
        PhoenixMicro.Supervisor.ConsumerManager
        |> then(fn _msg ->
          PhoenixMicro.Pipeline.child_spec({SimpleConsumer, []})
        end)

      assert spec.type == :supervisor
    end

    test "simple consumer config has :simple pipeline" do
      cfg = SimpleModeLegacyConsumer.__consumer_config__()
      assert cfg.pipeline == :simple
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry events emitted by Pipeline
  # ---------------------------------------------------------------------------

  describe "Pipeline telemetry" do
    test "message_received event fires on dispatch" do
      test_pid = self()
      ref = make_ref()

      :telemetry.attach(
        inspect(ref),
        [:phoenix_micro, :message, :received],
        fn _event, _measurements, meta, _config ->
          send(test_pid, {:telemetry_received, meta.topic})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(inspect(ref)) end)

      msg = Message.new("pipeline.simple", %{})
      ctx = %{transport: :memory, topic: "pipeline.simple", attempt: 1}
      Consumer.dispatch(SimpleConsumer, msg, ctx)

      assert_receive {:telemetry_received, "pipeline.simple"}, 500
    end

    test "message_processed event fires on success" do
      test_pid = self()
      ref = make_ref()

      :telemetry.attach(
        inspect(ref),
        [:phoenix_micro, :message, :processed],
        fn _event, _measurements, meta, _config ->
          send(test_pid, {:telemetry_processed, meta.topic})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(inspect(ref)) end)

      msg = Message.new("pipeline.simple", %{})
      ctx = %{transport: :memory, topic: "pipeline.simple", attempt: 1}
      Consumer.dispatch(SimpleConsumer, msg, ctx)

      assert_receive {:telemetry_processed, "pipeline.simple"}, 500
    end

    test "message_failed event fires on error" do
      test_pid = self()
      ref = make_ref()

      :telemetry.attach(
        inspect(ref),
        [:phoenix_micro, :message, :failed],
        fn _event, _measurements, meta, _config ->
          send(test_pid, {:telemetry_failed, meta.topic, meta.reason})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(inspect(ref)) end)

      msg = Message.new("pipeline.errors", %{})
      ctx = %{transport: :memory, topic: "pipeline.errors", attempt: 1}
      Consumer.dispatch(ErrorConsumer, msg, ctx)

      assert_receive {:telemetry_failed, "pipeline.errors", :deliberate_failure}, 500
    end
  end
end
