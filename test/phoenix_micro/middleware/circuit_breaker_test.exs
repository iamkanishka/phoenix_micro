defmodule PhoenixMicro.Middleware.CircuitBreakerTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Message
  alias PhoenixMicro.Middleware.CircuitBreaker
  alias PhoenixMicro.Middleware.CircuitBreaker.Store

  setup do
    start_supervised!(Store)
    Store.reset_all()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ok_handler(_msg), do: :ok
  defp error_handler(_msg), do: {:error, :downstream_failure}

  defp call(msg, handler, opts \\ []) do
    CircuitBreaker.call(msg, handler, opts)
  end

  defp msg(topic \\ "test.topic") do
    Message.new(topic, %{})
  end

  defp trip_breaker(fuse, threshold) do
    for _i <- 1..threshold do
      call(msg(fuse), &error_handler/1,
        fuse: fuse,
        threshold: threshold,
        window_ms: 60_000,
        reset_timeout_ms: 60_000
      )
    end
  end

  # ---------------------------------------------------------------------------
  # CLOSED state
  # ---------------------------------------------------------------------------

  describe "CLOSED state" do
    test "passes successful messages through" do
      result = call(msg(), &ok_handler/1, fuse: "healthy.fuse", threshold: 5)
      assert result == :ok
    end

    test "returns error from handler without tripping below threshold" do
      result =
        call(msg("fragile.fuse"), &error_handler/1,
          fuse: "fragile.fuse",
          threshold: 5,
          window_ms: 60_000
        )

      assert result == {:error, :downstream_failure}
      assert Store.state("fragile.fuse") == :closed
    end

    test "trips to OPEN after threshold failures" do
      fuse = "trips.fuse"
      threshold = 3

      for _i <- 1..(threshold - 1) do
        call(msg(fuse), &error_handler/1,
          fuse: fuse,
          threshold: threshold,
          window_ms: 60_000,
          reset_timeout_ms: 60_000
        )
      end

      assert Store.state(fuse) == :closed

      # This one tips it over
      call(msg(fuse), &error_handler/1,
        fuse: fuse,
        threshold: threshold,
        window_ms: 60_000,
        reset_timeout_ms: 60_000
      )

      assert {:open, _opened_at} = Store.state(fuse)
    end

    test "success resets failure window" do
      fuse = "reset.on.success"
      threshold = 3

      # 2 failures — below threshold
      for _i <- 1..2 do
        call(msg(fuse), &error_handler/1, fuse: fuse, threshold: threshold, window_ms: 60_000)
      end

      # Success resets
      call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: threshold)

      assert Store.state(fuse) == :closed

      # Now 2 more failures shouldn't trip either (window was cleared)
      for _i <- 1..2 do
        call(msg(fuse), &error_handler/1, fuse: fuse, threshold: threshold, window_ms: 60_000)
      end

      assert Store.state(fuse) == :closed
    end
  end

  # ---------------------------------------------------------------------------
  # OPEN state
  # ---------------------------------------------------------------------------

  describe "OPEN state" do
    test "rejects messages immediately without calling handler" do
      fuse = "open.fuse"
      call_count = :counters.new(1, [])

      trip_breaker(fuse, 3)
      assert {:open, _ts} = Store.state(fuse)

      result =
        call(
          msg(fuse),
          fn _msg ->
            :counters.add(call_count, 1, 1)
            :ok
          end,
          fuse: fuse,
          threshold: 3,
          reset_timeout_ms: 60_000
        )

      assert result == {:error, :circuit_open}
      assert :counters.get(call_count, 1) == 0
    end

    test "transitions to HALF_OPEN after reset_timeout" do
      fuse = "timeout.fuse"
      trip_breaker(fuse, 3)

      # Force the store into OPEN state with an old timestamp
      past = System.monotonic_time(:millisecond) - 100
      :ets.insert(:phoenix_micro_circuit_breakers, {fuse, {:open, past}, [], past})

      # Next call should see elapsed >= reset_timeout (10ms) and switch to HALF_OPEN
      result = call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3, reset_timeout_ms: 10)

      # The probe succeeded, so it should now be CLOSED
      assert result == :ok
      assert Store.state(fuse) == :closed
    end

    test "remains OPEN when reset_timeout has not elapsed" do
      fuse = "still.open"
      trip_breaker(fuse, 3)

      # Just tripped — reset_timeout of 60s has not elapsed
      result = call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3, reset_timeout_ms: 60_000)

      assert result == {:error, :circuit_open}
      assert {:open, _ts} = Store.state(fuse)
    end
  end

  # ---------------------------------------------------------------------------
  # HALF_OPEN state
  # ---------------------------------------------------------------------------

  describe "HALF_OPEN state" do
    test "allows a single probe through" do
      fuse = "half.open.fuse"
      Store.set_open(fuse)
      Store.set_half_open(fuse)

      assert Store.state(fuse) == :half_open

      probe_called = :counters.new(1, [])

      call(
        msg(fuse),
        fn _msg ->
          :counters.add(probe_called, 1, 1)
          :ok
        end,
        fuse: fuse,
        threshold: 3,
        reset_timeout_ms: 60_000
      )

      assert :counters.get(probe_called, 1) == 1
    end

    test "successful probe resets to CLOSED" do
      fuse = "probe.success"
      Store.set_open(fuse)
      Store.set_half_open(fuse)

      call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3, reset_timeout_ms: 1)

      assert Store.state(fuse) == :closed
    end

    test "failed probe returns to OPEN" do
      fuse = "probe.failure"
      Store.set_open(fuse)
      Store.set_half_open(fuse)

      call(msg(fuse), &error_handler/1, fuse: fuse, threshold: 3, reset_timeout_ms: 60_000)

      assert {:open, _ts} = Store.state(fuse)
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry events
  # ---------------------------------------------------------------------------

  describe "Telemetry events" do
    setup do
      test_pid = self()
      ref = make_ref()

      events = [
        [:phoenix_micro, :circuit_breaker, :tripped],
        [:phoenix_micro, :circuit_breaker, :reset],
        [:phoenix_micro, :circuit_breaker, :rejected],
        [:phoenix_micro, :circuit_breaker, :probe]
      ]

      :telemetry.attach_many(
        inspect(ref),
        events,
        fn event, _measurements, meta, _config ->
          send(test_pid, {:cb_event, List.last(event), meta})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(inspect(ref)) end)
      :ok
    end

    test "emits :tripped when breaker opens" do
      fuse = "telemetry.trip"
      trip_breaker(fuse, 3)

      assert_receive {:cb_event, :tripped, %{fuse: ^fuse}}, 500
    end

    test "emits :rejected when message is refused in OPEN state" do
      fuse = "telemetry.reject"
      trip_breaker(fuse, 3)

      call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3, reset_timeout_ms: 60_000)

      assert_receive {:cb_event, :rejected, %{fuse: ^fuse}}, 500
    end

    test "emits :probe when HALF_OPEN sends probe" do
      fuse = "telemetry.probe"
      Store.set_open(fuse)
      Store.set_half_open(fuse)

      call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3)

      assert_receive {:cb_event, :probe, %{fuse: ^fuse}}, 500
    end

    test "emits :reset when probe succeeds" do
      fuse = "telemetry.reset"
      Store.set_open(fuse)
      Store.set_half_open(fuse)

      call(msg(fuse), &ok_handler/1, fuse: fuse, threshold: 3)

      assert_receive {:cb_event, :reset, %{fuse: ^fuse}}, 500
    end
  end

  # ---------------------------------------------------------------------------
  # Store — unit tests
  # ---------------------------------------------------------------------------

  describe "CircuitBreaker.Store" do
    test "fresh fuse is CLOSED" do
      assert Store.state("brand.new") == :closed
    end

    test "set_open / set_closed round-trip" do
      Store.set_open("toggled")
      assert {:open, _ts} = Store.state("toggled")

      Store.set_closed("toggled")
      assert Store.state("toggled") == :closed
    end

    test "set_half_open" do
      Store.set_open("half")
      Store.set_half_open("half")
      assert Store.state("half") == :half_open
    end

    test "record_failure returns incrementing count" do
      fuse = "counting"
      assert Store.record_failure(fuse, 60_000) == 1
      assert Store.record_failure(fuse, 60_000) == 2
      assert Store.record_failure(fuse, 60_000) == 3
    end

    test "record_failure prunes old failures outside window" do
      fuse = "pruned"
      # Record a failure 10 seconds in the past by inserting directly
      old_ts = System.monotonic_time(:millisecond) - 10_001
      :ets.insert(:phoenix_micro_circuit_breakers, {fuse, :closed, [old_ts, old_ts], nil})

      # Window of 10 seconds — old failures should be pruned
      count = Store.record_failure(fuse, 10_000)
      assert count == 1
    end

    test "record_success clears failure window" do
      fuse = "cleared"
      Store.record_failure(fuse, 60_000)
      Store.record_failure(fuse, 60_000)
      Store.record_success(fuse)

      # Next failure starts fresh
      assert Store.record_failure(fuse, 60_000) == 1
    end

    test "all_states returns current snapshot" do
      Store.set_open("snap.a")
      Store.set_closed("snap.b")

      states = Store.all_states() |> Map.new()
      assert {:open, _ts} = states["snap.a"]
      assert :closed = states["snap.b"]
    end

    test "reset_all clears everything" do
      Store.set_open("clear.me")
      Store.reset_all()
      assert Store.all_states() == []
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple fuses are independent
  # ---------------------------------------------------------------------------

  describe "fuse isolation" do
    test "tripping one fuse does not affect another" do
      trip_breaker("isolated.a", 3)

      assert {:open, _ts} = Store.state("isolated.a")
      assert Store.state("isolated.b") == :closed

      result = call(msg("isolated.b"), &ok_handler/1, fuse: "isolated.b", threshold: 3)
      assert result == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent access
  # ---------------------------------------------------------------------------

  describe "concurrent failure recording" do
    test "handles concurrent record_failure calls without data race" do
      fuse = "concurrent.cb"

      tasks =
        for _i <- 1..50 do
          Task.async(fn -> Store.record_failure(fuse, 60_000) end)
        end

      results = Task.await_many(tasks)

      # All counts should be positive integers
      assert Enum.all?(results, &is_integer/1)
      # Final count should be 50
      assert Store.record_failure(fuse, 60_000) == 51
    end
  end
end
