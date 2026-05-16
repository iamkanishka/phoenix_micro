defmodule PhoenixMicro.Utils.WorkerPoolTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Utils.WorkerPool

  setup do
    name = :erlang.unique_integer([:positive, :monotonic])

    {:ok, _pid} =
      start_supervised({WorkerPool, name: name, max_concurrency: 5, max_queue_size: 20})

    %{pool: name}
  end

  describe "submit/2" do
    test "executes the function asynchronously", %{pool: pool} do
      test_pid = self()
      {:ok, _task} = WorkerPool.submit(pool, fn -> send(test_pid, :done) end)
      assert_receive :done, 1_000
    end

    test "returns {:ok, :queued} when all slots busy", %{pool: pool} do
      test_pid = self()

      # Fill all 5 slots with slow tasks
      for _i <- 1..5 do
        WorkerPool.submit(pool, fn ->
          Process.sleep(500)
          send(test_pid, :slot_done)
        end)
      end

      # 6th should be queued or immediately dispatched if a slot freed
      result = WorkerPool.submit(pool, fn -> :ok end)
      assert match?({:ok, _}, result)
    end

    test "returns {:error, :overloaded} when queue full", %{pool: pool} do
      # Fill slots + queue
      for _j <- 1..25 do
        WorkerPool.submit(pool, fn -> Process.sleep(200) end)
      end

      # Now overloaded
      assert {:error, :overloaded} = WorkerPool.submit(pool, fn -> :ok end)
    end

    test "processes queued tasks after slot frees", %{pool: pool} do
      test_pid = self()
      counter = :counters.new(1, [])

      for _k <- 1..10 do
        WorkerPool.submit(pool, fn ->
          :counters.add(counter, 1, 1)
          send(test_pid, :done)
        end)
      end

      for _i <- 1..10, do: assert_receive(:done, 2_000)
      assert :counters.get(counter, 1) == 10
    end
  end

  describe "submit_await/3" do
    test "returns the function's result", %{pool: pool} do
      assert {:ok, 42} = WorkerPool.submit_await(pool, fn -> 42 end)
    end

    test "returns {:error, :timeout} when function takes too long", %{pool: pool} do
      assert {:error, :timeout} = WorkerPool.submit_await(pool, fn -> Process.sleep(500) end, 50)
    end
  end

  describe "status/1" do
    test "returns active + pending + max_concurrency", %{pool: pool} do
      status = WorkerPool.status(pool)
      assert is_integer(status.active)
      assert is_integer(status.pending)
      assert status.max_concurrency == 5
    end
  end
end
