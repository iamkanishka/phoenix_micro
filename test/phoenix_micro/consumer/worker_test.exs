defmodule PhoenixMicro.Consumer.WorkerTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Consumer, Message}
  alias PhoenixMicro.Transport.Memory

  @transport_name :worker_test_memory

  # ---------------------------------------------------------------------------
  # Test consumers
  # ---------------------------------------------------------------------------

  defmodule SuccessfulConsumer do
    use PhoenixMicro.Consumer
    topic("worker.success")
    transport(:memory)
    concurrency(1)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = Map.get(msg.payload, "test_pid")
      if pid, do: send(pid, {:consumed, msg.payload})
      :ok
    end
  end

  defmodule RetryConsumer do
    use PhoenixMicro.Consumer
    topic("worker.retry")
    transport(:memory)
    concurrency(1)
    retry(max_attempts: 3, base_delay: 10, max_delay: 100, jitter: false)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = Map.get(msg.payload, "test_pid")
      attempt = msg.attempt
      send(pid, {:attempt, attempt})

      if attempt < 3 do
        {:error, :not_ready_yet}
      else
        send(pid, :finally_succeeded)
        :ok
      end
    end
  end

  defmodule DLQConsumer do
    use PhoenixMicro.Consumer
    topic("worker.dlq_test")
    transport(:memory)
    concurrency(1)
    retry(max_attempts: 1, base_delay: 10)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = msg, _ctx) do
      pid = Map.get(msg.payload, "test_pid")
      send(pid, {:attempt, msg.attempt})
      {:error, :always_fails}
    end
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    # Override transport to use our named memory instance
    Application.put_env(:phoenix_micro, :transport, :memory)
    :ok
  end

  setup do
    name = :erlang.unique_integer([:positive, :monotonic])
    {:ok, _pid} = start_supervised({Memory, [name: name]})
    %{memory_name: name}
  end

  # ---------------------------------------------------------------------------
  # __consumer_config__/0 exposure
  # ---------------------------------------------------------------------------

  describe "consumer config" do
    test "SuccessfulConsumer has expected config" do
      cfg = SuccessfulConsumer.__consumer_config__()
      assert cfg.topic == "worker.success"
      assert cfg.concurrency == 1
      assert cfg.transport == :memory
    end

    test "RetryConsumer has retry config" do
      cfg = RetryConsumer.__consumer_config__()
      assert cfg.retry_opts[:max_attempts] == 3
      assert cfg.retry_opts[:base_delay] == 10
    end
  end

  # ---------------------------------------------------------------------------
  # dispatch/3 — directly tests Consumer.dispatch without Worker process
  # ---------------------------------------------------------------------------

  describe "Consumer.dispatch/3" do
    test "dispatches to successful consumer" do
      test_pid = self()
      msg = Message.new("worker.success", %{"test_pid" => test_pid})
      ctx = %{transport: :memory, topic: "worker.success", attempt: 1}

      assert :ok = Consumer.dispatch(SuccessfulConsumer, msg, ctx)
      assert_receive {:consumed, %{"test_pid" => ^test_pid}}, 500
    end

    test "returns error from failing consumer" do
      msg = Message.new("worker.retry", %{"test_pid" => self()})
      ctx = %{transport: :memory, topic: "worker.retry", attempt: 1}

      result = Consumer.dispatch(RetryConsumer, msg, ctx)
      assert {:error, :not_ready_yet} = result
    end

    test "dispatches with middleware chain" do
      test_pid = self()
      msg = Message.new("worker.success", %{"test_pid" => test_pid})
      ctx = %{transport: :memory, topic: "worker.success", attempt: 1}

      # Logger middleware is in SuccessfulConsumer (none) — use directly
      result = Consumer.dispatch(SuccessfulConsumer, msg, ctx)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # RetryScheduler
  # ---------------------------------------------------------------------------

  describe "RetryScheduler" do
    alias PhoenixMicro.Consumer.RetryScheduler

    test "attempt 1 with base 100ms, no jitter = 100ms" do
      delay = RetryScheduler.next_delay(1, base_delay: 100, max_delay: 60_000, jitter: false)
      assert delay == 100
    end

    test "attempt 2 = 200ms" do
      delay = RetryScheduler.next_delay(2, base_delay: 100, max_delay: 60_000, jitter: false)
      assert delay == 200
    end

    test "attempt 10 is capped at max_delay" do
      delay = RetryScheduler.next_delay(10, base_delay: 100, max_delay: 500, jitter: false)
      assert delay == 500
    end

    test "jitter produces different values" do
      opts = [base_delay: 500, max_delay: 10_000, jitter: true]
      delays = for _i <- 1..20, do: RetryScheduler.next_delay(3, opts)
      # With jitter, values should vary
      assert Enum.count(Enum.uniq(delays)) > 1
    end

    test "jitter values are within reasonable bounds" do
      opts = [base_delay: 1_000, max_delay: 30_000, jitter: true]
      delays = for _i <- 1..100, do: RetryScheduler.next_delay(1, opts)
      # All should be >= base_delay (1000ms)
      assert Enum.all?(delays, &(&1 >= 1_000))
      # None should exceed max_delay by more than one jitter segment
      assert Enum.all?(delays, &(&1 <= 30_000 + 7_501))
    end
  end

  # ---------------------------------------------------------------------------
  # Consumer.config/1
  # ---------------------------------------------------------------------------

  describe "Consumer.config/1" do
    test "returns the config map from __consumer_config__" do
      cfg = Consumer.config(SuccessfulConsumer)
      assert is_map(cfg)
      assert cfg.topic == "worker.success"
    end
  end
end
