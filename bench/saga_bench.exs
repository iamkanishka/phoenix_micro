#!/usr/bin/env elixir
# Run with: mix run bench/saga_bench.exs

Application.ensure_all_started(:phoenix_micro)

alias PhoenixMicro.{Saga, Message}

IO.puts("Starting Saga benchmarks...\n")

# ---------------------------------------------------------------------------
# Shared saga definitions
# ---------------------------------------------------------------------------

defmodule SagaBench.FastSaga do
  use PhoenixMicro.Saga

  step :step_a,
    execute: fn ctx -> {:ok, Map.put(ctx, :a, true)} end,
    compensate: fn _ctx -> :ok end

  step :step_b,
    execute: fn ctx -> {:ok, Map.put(ctx, :b, true)} end,
    compensate: fn _ctx -> :ok end

  step :step_c,
    execute: fn ctx -> {:ok, Map.put(ctx, :c, true)} end
end

defmodule SagaBench.FailingSaga do
  use PhoenixMicro.Saga

  step :step_a,
    execute: fn ctx -> {:ok, Map.put(ctx, :a, 1)} end,
    compensate: fn _ctx -> :ok end

  step :step_b,
    execute: fn ctx -> {:ok, Map.put(ctx, :b, 2)} end,
    compensate: fn _ctx -> :ok end

  step :step_c,
    execute: fn _ctx -> {:error, :deliberate_failure} end
end

defmodule SagaBench.RetrySaga do
  use PhoenixMicro.Saga

  step :flaky,
    execute: fn ctx ->
      n = Map.get(ctx, :attempts, 0) + 1
      ctx = Map.put(ctx, :attempts, n)
      if n < 2, do: {:error, :transient}, else: {:ok, ctx}
    end,
    retries: 3
end

# ---------------------------------------------------------------------------
# Benchmark: saga step compilation
# ---------------------------------------------------------------------------

compile_suite = %{
  "__saga_steps__/0 (3 steps)" => fn ->
    SagaBench.FastSaga.__saga_steps__()
  end
}

# ---------------------------------------------------------------------------
# Benchmark: inline saga execution (no process overhead)
# The saga server logic is exercised via run/3 in a Task to avoid
# polluting the main process's mailbox.
# ---------------------------------------------------------------------------

run_opts = [timeout: 5_000]

execution_suite = %{
  "Saga.run/3 — happy path (3 steps)" => fn ->
    id = Message.generate_id()
    Task.await(Task.async(fn ->
      Saga.run(SagaBench.FastSaga, %{}, Keyword.put(run_opts, :id, id))
    end), 5_000)
  end,
  "Saga.run/3 — failure + 2 compensations" => fn ->
    id = Message.generate_id()
    Task.await(Task.async(fn ->
      Saga.run(SagaBench.FailingSaga, %{}, Keyword.put(run_opts, :id, id))
    end), 5_000)
  end,
  "Saga.run/3 — 1 retry then succeed" => fn ->
    id = Message.generate_id()
    Task.await(Task.async(fn ->
      Saga.run(SagaBench.RetrySaga, %{}, Keyword.put(run_opts, :id, id))
    end), 5_000)
  end
}

# ---------------------------------------------------------------------------
# Benchmark: concurrent sagas
# ---------------------------------------------------------------------------

concurrency_suite = %{
  "10 concurrent sagas (happy path)" => fn ->
    tasks = for _ <- 1..10 do
      id = Message.generate_id()
      Task.async(fn ->
        Saga.run(SagaBench.FastSaga, %{}, Keyword.put(run_opts, :id, id))
      end)
    end

    Task.await_many(tasks, 10_000)
  end
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

all_suites = [
  {"Saga Compilation",    compile_suite},
  {"Saga Execution",      execution_suite},
  {"Saga Concurrency",    concurrency_suite}
]

benchee_opts = [
  warmup: 1,
  time: 3,
  memory_time: 1,
  print: [fast_warning: false],
  formatters: [Benchee.Formatters.Console]
]

Enum.each(all_suites, fn {name, suite} ->
  IO.puts("\n#{"=" |> String.duplicate(60)}")
  IO.puts("Suite: #{name}")
  IO.puts("=" |> String.duplicate(60))
  Benchee.run(suite, benchee_opts)
end)
