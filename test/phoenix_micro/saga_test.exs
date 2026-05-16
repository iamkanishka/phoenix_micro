defmodule PhoenixMicro.SagaTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.{Message, Saga}
  alias PhoenixMicro.Saga.{Server, Step}

  # ---------------------------------------------------------------------------
  # Test saga definitions
  # ---------------------------------------------------------------------------

  defmodule HappyPathSaga do
    use PhoenixMicro.Saga

    step(:step_one,
      execute: fn ctx ->
        {:ok, Map.put(ctx, :step_one_done, true)}
      end,
      compensate: fn ctx ->
        send(Map.get(ctx, :test_pid), :compensated_step_one)
        :ok
      end
    )

    step(:step_two,
      execute: fn ctx ->
        {:ok, Map.put(ctx, :step_two_done, true)}
      end,
      compensate: fn ctx ->
        send(Map.get(ctx, :test_pid), :compensated_step_two)
        :ok
      end
    )

    step(:step_three,
      execute: fn ctx ->
        send(Map.get(ctx, :test_pid), :step_three_executed)
        {:ok, Map.put(ctx, :step_three_done, true)}
      end
    )
  end

  defmodule FailOnStepTwoSaga do
    use PhoenixMicro.Saga

    step(:step_one,
      execute: fn ctx ->
        {:ok, Map.put(ctx, :one_result, "reserved")}
      end,
      compensate: fn ctx ->
        send(Map.get(ctx, :test_pid), {:compensated, :step_one})
        :ok
      end
    )

    step(:step_two,
      execute: fn _saga_ctx ->
        {:error, :payment_declined}
      end,
      compensate: fn ctx ->
        # Should NOT be called — step_two never completed
        send(Map.get(ctx, :test_pid), {:compensated, :step_two})
        :ok
      end
    )

    step(:step_three,
      execute: fn ctx ->
        send(Map.get(ctx, :test_pid), :step_three_should_not_run)
        {:ok, ctx}
      end
    )
  end

  defmodule NoCompensationSaga do
    use PhoenixMicro.Saga

    step(:notify,
      execute: fn ctx ->
        send(Map.get(ctx, :test_pid), :notified)
        {:ok, ctx}
      end
    )

    # No compensate — fire-and-forget
  end

  defmodule RetryingSaga do
    use PhoenixMicro.Saga

    step(:flaky_step,
      execute: fn ctx ->
        count = Map.get(ctx, :attempt_count, 0) + 1
        ctx = Map.put(ctx, :attempt_count, count)

        if count < 3 do
          {:error, :transient}
        else
          {:ok, Map.put(ctx, :eventually_succeeded, true)}
        end
      end,
      retries: 3
    )
  end

  defmodule BadCompensationSaga do
    use PhoenixMicro.Saga

    step(:step_one,
      execute: fn ctx -> {:ok, ctx} end,
      compensate: fn _saga_ctx -> {:error, :compensation_boom} end
    )

    step(:step_two,
      execute: fn _saga_ctx -> {:error, :trigger_rollback} end
    )
  end

  # ---------------------------------------------------------------------------
  # Setup — start the Registry and Saga.Supervisor
  # ---------------------------------------------------------------------------

  setup do
    # Start required processes for saga tests
    start_supervised!({Registry, keys: :unique, name: PhoenixMicro.Registry})
    start_supervised!(PhoenixMicro.Saga.Supervisor)
    :ok
  rescue
    # Already started from another test or supervision tree
    _e -> :ok
  end

  # ---------------------------------------------------------------------------
  # __saga_steps__/0
  # ---------------------------------------------------------------------------

  describe "__saga_steps__/0" do
    test "returns steps in definition order" do
      steps = HappyPathSaga.__saga_steps__()
      names = Enum.map(steps, & &1.name)
      assert names == [:step_one, :step_two, :step_three]
    end

    test "steps are Step structs" do
      [step | _rest] = HappyPathSaga.__saga_steps__()
      assert %Step{name: :step_one} = step
      assert is_function(step.execute, 1)
      assert is_function(step.compensate, 1)
    end

    test "step without compensate has nil compensate" do
      [step] = NoCompensationSaga.__saga_steps__()
      assert is_nil(step.compensate)
    end

    test "RetryingSaga step has retries: 3" do
      [step] = RetryingSaga.__saga_steps__()
      assert step.retries == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Happy path
  # ---------------------------------------------------------------------------

  describe "Saga.run/3 — happy path" do
    test "runs all steps and returns {:ok, final_context}" do
      test_pid = self()
      ctx = %{test_pid: test_pid}

      result =
        run_in_process(fn ->
          Saga.run(HappyPathSaga, ctx, id: Message.generate_id())
        end)

      assert {:ok, final} = result
      assert final.step_one_done == true
      assert final.step_two_done == true
      assert final.step_three_done == true

      assert_receive :step_three_executed, 1_000
    end

    test "each step can add data to context" do
      result =
        run_in_process(fn ->
          Saga.run(HappyPathSaga, %{}, id: Message.generate_id())
        end)

      assert {:ok, ctx} = result
      assert ctx.step_one_done
      assert ctx.step_two_done
      assert ctx.step_three_done
    end

    test "no compensations run on success" do
      test_pid = self()

      run_in_process(fn ->
        Saga.run(HappyPathSaga, %{test_pid: test_pid}, id: Message.generate_id())
      end)

      refute_receive :compensated_step_one, 300
      refute_receive :compensated_step_two, 300
    end
  end

  # ---------------------------------------------------------------------------
  # Failure + compensation
  # ---------------------------------------------------------------------------

  describe "Saga.run/3 — failure + compensation" do
    test "returns {:compensated, reason, context} when a step fails" do
      test_pid = self()
      ctx = %{test_pid: test_pid}

      result =
        run_in_process(fn ->
          Saga.run(FailOnStepTwoSaga, ctx, id: Message.generate_id())
        end)

      assert {:compensated, :payment_declined, _final_ctx} = result
    end

    test "compensates completed steps in reverse order" do
      test_pid = self()
      ctx = %{test_pid: test_pid}

      run_in_process(fn ->
        Saga.run(FailOnStepTwoSaga, ctx, id: Message.generate_id())
      end)

      # Step one completed, so it should be compensated
      assert_receive {:compensated, :step_one}, 1_000

      # Step two never completed, so its compensation should NOT run
      refute_receive {:compensated, :step_two}, 300
    end

    test "steps after the failed step do not execute" do
      test_pid = self()
      ctx = %{test_pid: test_pid}

      run_in_process(fn ->
        Saga.run(FailOnStepTwoSaga, ctx, id: Message.generate_id())
      end)

      refute_receive :step_three_should_not_run, 300
    end

    test "step without compensate is skipped during rollback" do
      test_pid = self()

      defmodule NoCompFailSaga do
        use PhoenixMicro.Saga

        step(:fire_and_forget,
          execute: fn ctx ->
            send(Map.get(ctx, :test_pid), :fired)
            {:ok, ctx}
          end
        )

        step(:always_fails,
          execute: fn _saga_ctx -> {:error, :boom} end
        )
      end

      result =
        run_in_process(fn ->
          Saga.run(NoCompFailSaga, %{test_pid: test_pid}, id: Message.generate_id())
        end)

      assert {:compensated, :boom, _ctx} = result
      assert_receive :fired, 500
    end
  end

  # ---------------------------------------------------------------------------
  # Compensation failure
  # ---------------------------------------------------------------------------

  describe "Saga.run/3 — compensation failure" do
    test "returns {:error, :compensation_failed, details} when compensation fails" do
      result =
        run_in_process(fn ->
          Saga.run(BadCompensationSaga, %{}, id: Message.generate_id())
        end)

      assert {:error, :compensation_failed, _details} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Retries within a step
  # ---------------------------------------------------------------------------

  describe "step retries" do
    test "retries step up to configured count before failing" do
      result =
        run_in_process(fn ->
          Saga.run(RetryingSaga, %{}, id: Message.generate_id())
        end)

      assert {:ok, ctx} = result
      assert ctx.eventually_succeeded == true
      assert ctx.attempt_count == 3
    end
  end

  # ---------------------------------------------------------------------------
  # Async start + status
  # ---------------------------------------------------------------------------

  describe "Saga.start/3 + status/1" do
    test "start returns saga_id immediately" do
      {:ok, saga_id} = Saga.start(HappyPathSaga, %{})
      assert is_binary(saga_id)
    end

    test "status/1 returns :not_found for unknown saga" do
      assert {:error, :not_found} = Saga.status("does-not-exist")
    end

    test "status/1 returns running or completed status" do
      {:ok, saga_id} = Saga.start(HappyPathSaga, %{})
      Process.sleep(100)

      case Saga.status(saga_id) do
        {:ok, %{status: status}} ->
          assert status in [:running, :completed]

        {:error, :not_found} ->
          # Saga completed and process exited — also acceptable
          :ok
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry
  # ---------------------------------------------------------------------------

  describe "Saga telemetry" do
    setup do
      test_pid = self()
      ref = make_ref()

      events = [
        [:phoenix_micro, :saga, :started],
        [:phoenix_micro, :saga, :step_started],
        [:phoenix_micro, :saga, :step_ok],
        [:phoenix_micro, :saga, :step_failed],
        [:phoenix_micro, :saga, :completed],
        [:phoenix_micro, :saga, :compensating],
        [:phoenix_micro, :saga, :compensated]
      ]

      :telemetry.attach_many(
        inspect(ref),
        events,
        fn event, _measurements, meta, _config ->
          send(test_pid, {:saga_telemetry, List.last(event), meta})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(inspect(ref)) end)
      :ok
    end

    test "emits :started and :completed on happy path" do
      run_in_process(fn ->
        Saga.run(HappyPathSaga, %{}, id: Message.generate_id())
      end)

      assert_receive {:saga_telemetry, :started, _}, 1_000
      assert_receive {:saga_telemetry, :completed, _}, 1_000
    end

    test "emits :step_started and :step_ok for each step" do
      run_in_process(fn ->
        Saga.run(NoCompensationSaga, %{test_pid: self()}, id: Message.generate_id())
      end)

      assert_receive {:saga_telemetry, :step_started, %{step: :notify}}, 1_000
      assert_receive {:saga_telemetry, :step_ok, %{step: :notify}}, 1_000
    end

    test "emits :step_failed, :compensating, :compensated on failure" do
      run_in_process(fn ->
        Saga.run(FailOnStepTwoSaga, %{test_pid: self()}, id: Message.generate_id())
      end)

      assert_receive {:saga_telemetry, :step_failed, %{step: :step_two}}, 1_000
      assert_receive {:saga_telemetry, :compensating, _}, 1_000
      assert_receive {:saga_telemetry, :compensated, _}, 1_000
    end
  end

  # ---------------------------------------------------------------------------
  # Step struct
  # ---------------------------------------------------------------------------

  describe "Step struct" do
    test "requires :name and :execute" do
      step = %Step{name: :my_step, execute: fn ctx -> {:ok, ctx} end}
      assert step.name == :my_step
      assert is_function(step.execute, 1)
      assert is_nil(step.compensate)
      assert step.timeout == 30_000
      assert step.retries == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Helper — run saga in a temporary process so Registry listeners work
  # ---------------------------------------------------------------------------

  defp run_in_process(fun) do
    task = Task.async(fun)
    Task.await(task, 5_000)
  end
end
