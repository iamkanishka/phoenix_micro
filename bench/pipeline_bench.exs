#!/usr/bin/env elixir
# Run with: mix run bench/pipeline_bench.exs

# Ensure the app is started before benchmarking
Application.ensure_all_started(:phoenix_micro)

alias PhoenixMicro.{Message, Consumer}
alias PhoenixMicro.Transport.Memory
alias PhoenixMicro.Middleware.CircuitBreaker
alias PhoenixMicro.Middleware.CircuitBreaker.Store, as: CBStore
alias PhoenixMicro.Consumer.RetryScheduler

IO.puts("Starting PhoenixMicro benchmarks...\n")

# ---------------------------------------------------------------------------
# Shared setup
# ---------------------------------------------------------------------------

{:ok, _} = Memory.start_link(name: :bench_memory)
CBStore.start_link([])

# ---------------------------------------------------------------------------
# Benchmark 1: Message struct creation and UUID generation
# ---------------------------------------------------------------------------

message_suite = %{
  "Message.new/2" => fn ->
    Message.new("bench.topic", %{id: 1, data: "hello"})
  end,
  "Message.generate_id/0" => fn ->
    Message.generate_id()
  end,
  "Message.increment_attempt/1" => fn ->
    msg = Message.new("bench.topic", %{})
    Message.increment_attempt(msg)
  end
}

# ---------------------------------------------------------------------------
# Benchmark 2: Consumer dispatch through middleware chain
# ---------------------------------------------------------------------------

defmodule BenchConsumer do
  use PhoenixMicro.Consumer
  topic "bench.consumer"
  concurrency 1
  middleware []

  @impl PhoenixMicro.Consumer
  def handle(_message, _ctx), do: :ok
end

defmodule BenchConsumerWithMiddleware do
  use PhoenixMicro.Consumer
  topic "bench.consumer.mw"
  concurrency 1
  middleware [
    PhoenixMicro.Middleware.Logger,
    PhoenixMicro.Middleware.Metrics
  ]

  @impl PhoenixMicro.Consumer
  def handle(_message, _ctx), do: :ok
end

bench_msg = Message.new("bench.consumer", %{payload: "bench_payload"})
bench_ctx = %{transport: :memory, topic: "bench.consumer", attempt: 1}

dispatch_suite = %{
  "Consumer.dispatch/3 (no middleware)" => fn ->
    Consumer.dispatch(BenchConsumer, bench_msg, bench_ctx)
  end,
  "Consumer.dispatch/3 (Logger + Metrics)" => fn ->
    Consumer.dispatch(BenchConsumerWithMiddleware, bench_msg, bench_ctx)
  end
}

# ---------------------------------------------------------------------------
# Benchmark 3: Memory transport publish/subscribe
# ---------------------------------------------------------------------------

# Subscribe once and reuse
test_pid = self()
GenServer.call(:bench_memory, {:subscribe, "bench.throughput", fn _msg ->
  send(test_pid, :delivered)
  :ok
end, []})

throughput_suite = %{
  "Memory.publish (fire-and-forget)" => fn ->
    msg = Message.new("bench.throughput", %{n: 1})
    GenServer.call(:bench_memory, {:publish, msg})
  end
}

# ---------------------------------------------------------------------------
# Benchmark 4: Circuit breaker state checks
# ---------------------------------------------------------------------------

CBStore.set_closed("bench.closed.fuse")
CBStore.set_open("bench.open.fuse")

noop_handler = fn _msg -> :ok end
bench_msg_cb = Message.new("bench.cb", %{})

cb_suite = %{
  "CircuitBreaker.call (CLOSED, success)" => fn ->
    CircuitBreaker.call(bench_msg_cb, noop_handler,
      fuse: "bench.closed.fuse",
      threshold: 10,
      window_ms: 60_000,
      reset_timeout_ms: 60_000
    )
  end,
  "CircuitBreaker.call (OPEN, rejected)" => fn ->
    CircuitBreaker.call(bench_msg_cb, noop_handler,
      fuse: "bench.open.fuse",
      threshold: 10,
      window_ms: 60_000,
      reset_timeout_ms: 60_000_000   # will never reset in bench
    )
  end,
  "CBStore.state/1" => fn ->
    CBStore.state("bench.closed.fuse")
  end,
  "CBStore.record_failure/2" => fn ->
    CBStore.record_failure("bench.failure.fuse", 60_000)
  end
}

# ---------------------------------------------------------------------------
# Benchmark 5: RetryScheduler
# ---------------------------------------------------------------------------

retry_suite = %{
  "RetryScheduler.next_delay (attempt 1, no jitter)" => fn ->
    RetryScheduler.next_delay(1, base_delay: 500, max_delay: 30_000, jitter: false)
  end,
  "RetryScheduler.next_delay (attempt 5, with jitter)" => fn ->
    RetryScheduler.next_delay(5, base_delay: 500, max_delay: 30_000, jitter: true)
  end
}

# ---------------------------------------------------------------------------
# Run all benchmarks
# ---------------------------------------------------------------------------

all_suites = [
  {"Message Creation", message_suite},
  {"Consumer Dispatch", dispatch_suite},
  {"Memory Transport", throughput_suite},
  {"Circuit Breaker",  cb_suite},
  {"Retry Scheduler",  retry_suite}
]

benchee_opts = [
  warmup: 1,
  time: 3,
  memory_time: 1,
  reduction_time: 1,
  print: [fast_warning: false],
  formatters: [
    Benchee.Formatters.Console
  ]
]

Enum.each(all_suites, fn {name, suite} ->
  IO.puts("\n#{"=" |> String.duplicate(60)}")
  IO.puts("Suite: #{name}")
  IO.puts("=" |> String.duplicate(60))
  Benchee.run(suite, benchee_opts)
end)
