defmodule PhoenixMicro.ConsumerTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.{Consumer, Message}

  # ---------------------------------------------------------------------------
  # Test consumer modules defined inline
  # ---------------------------------------------------------------------------

  defmodule BasicConsumer do
    use PhoenixMicro.Consumer

    topic("orders.created")
    concurrency(5)
    retry(max_attempts: 3, base_delay: 100)

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = message, _context) do
      send(message.payload[:test_pid], {:handled, message})
      :ok
    end
  end

  defmodule FailingConsumer do
    use PhoenixMicro.Consumer

    topic("orders.failing")
    concurrency(1)
    retry(max_attempts: 2, base_delay: 10)

    @impl PhoenixMicro.Consumer
    def handle(_message, _context) do
      {:error, :intentional_failure}
    end
  end

  defmodule CustomErrorConsumer do
    use PhoenixMicro.Consumer

    topic("orders.custom_error")
    concurrency(1)

    @impl PhoenixMicro.Consumer
    def handle(_message, _context) do
      {:error, :deliberate}
    end

    @impl PhoenixMicro.Consumer
    def handle_error(_message, _error, _context) do
      :nack
    end
  end

  defmodule MiddlewareConsumer do
    use PhoenixMicro.Consumer

    topic("orders.middleware")
    middleware([PhoenixMicro.Middleware.Logger])

    @impl PhoenixMicro.Consumer
    def handle(%Message{} = message, _context) do
      send(message.payload[:test_pid], :handled)
      :ok
    end
  end

  defmodule FullConfigConsumer do
    use PhoenixMicro.Consumer

    topic("payments.processed")
    concurrency(20)
    retry(max_attempts: 5, base_delay: 1_000, max_delay: 60_000, jitter: false)
    middleware([PhoenixMicro.Middleware.Metrics])
    dead_letter_topic("dlq.payments.processed")
    transport(:rabbitmq)
    queue_group("billing_team")

    @impl PhoenixMicro.Consumer
    def handle(_message, _context), do: :ok
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "__consumer_config__/0" do
    test "BasicConsumer exposes correct config" do
      cfg = BasicConsumer.__consumer_config__()

      assert cfg.topic == "orders.created"
      assert cfg.concurrency == 5
      assert cfg.retry_opts[:max_attempts] == 3
      assert cfg.retry_opts[:base_delay] == 100
      assert cfg.middleware == []
      assert is_nil(cfg.dlq_topic)
      assert is_nil(cfg.transport)
      assert is_nil(cfg.queue_group)
    end

    test "FullConfigConsumer exposes all overridden config" do
      cfg = FullConfigConsumer.__consumer_config__()

      assert cfg.topic == "payments.processed"
      assert cfg.concurrency == 20
      assert cfg.retry_opts[:max_attempts] == 5
      assert cfg.retry_opts[:base_delay] == 1_000
      assert cfg.retry_opts[:max_delay] == 60_000
      assert cfg.retry_opts[:jitter] == false
      assert cfg.middleware == [PhoenixMicro.Middleware.Metrics]
      assert cfg.dlq_topic == "dlq.payments.processed"
      assert cfg.transport == :rabbitmq
      assert cfg.queue_group == "billing_team"
    end

    test "defaults are applied for unset options" do
      cfg = BasicConsumer.__consumer_config__()
      # middleware defaults to []
      assert cfg.middleware == []
      # queue_group defaults to nil
      assert is_nil(cfg.queue_group)
    end
  end

  describe "dispatch/3" do
    test "calls handle/2 and returns :ok on success" do
      test_pid = self()
      msg = Message.new("orders.created", %{test_pid: test_pid})
      ctx = %{transport: :memory, topic: "orders.created", attempt: 1}

      result = Consumer.dispatch(BasicConsumer, msg, ctx)

      assert result == :ok
      assert_receive {:handled, ^msg}, 500
    end

    test "returns {:error, reason} when handle returns error" do
      msg = Message.new("orders.failing", %{})
      ctx = %{transport: :memory, topic: "orders.failing", attempt: 1}

      result = Consumer.dispatch(FailingConsumer, msg, ctx)
      assert result == {:error, :intentional_failure}
    end

    test "routes through middleware chain" do
      test_pid = self()
      msg = Message.new("orders.middleware", %{test_pid: test_pid})
      ctx = %{transport: :memory, topic: "orders.middleware", attempt: 1}

      # Logger middleware should not interfere
      result = Consumer.dispatch(MiddlewareConsumer, msg, ctx)

      assert result == :ok
      assert_receive :handled, 500
    end
  end

  describe "handle_error/3 default" do
    test "default returns {:retry, message}" do
      msg = Message.new("test", %{})
      ctx = %{}

      assert {:retry, ^msg} = BasicConsumer.handle_error(msg, :some_error, ctx)
    end

    test "custom handle_error returns :nack" do
      msg = Message.new("test", %{})
      ctx = %{}

      assert :nack = CustomErrorConsumer.handle_error(msg, :deliberate, ctx)
    end
  end

  describe "Consumer.RetryScheduler.next_delay/2" do
    alias PhoenixMicro.Consumer.RetryScheduler

    test "increases delay exponentially" do
      opts = [base_delay: 100, max_delay: 10_000, jitter: false]

      d1 = RetryScheduler.next_delay(1, opts)
      d2 = RetryScheduler.next_delay(2, opts)
      d3 = RetryScheduler.next_delay(3, opts)

      assert d1 == 100
      assert d2 == 200
      assert d3 == 400
    end

    test "caps at max_delay" do
      opts = [base_delay: 1_000, max_delay: 2_000, jitter: false]

      d5 = RetryScheduler.next_delay(5, opts)
      assert d5 == 2_000
    end

    test "adds jitter when enabled" do
      opts = [base_delay: 1_000, max_delay: 30_000, jitter: true]
      delays = for _i <- 1..50, do: RetryScheduler.next_delay(3, opts)

      # With jitter, not all delays should be identical
      assert Enum.count(Enum.uniq(delays)) > 1
      # All delays should be >= base
      assert Enum.all?(delays, &(&1 >= 400))
    end
  end
end
