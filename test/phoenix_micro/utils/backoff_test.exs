defmodule PhoenixMicro.Utils.BackoffTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.Utils.Backoff

  describe "next_delay/2 — no jitter" do
    test "attempt 1 returns base" do
      assert Backoff.next_delay(1, base: 100, cap: 10_000, jitter: false) == 100
    end

    test "doubles each attempt" do
      assert Backoff.next_delay(2, base: 100, cap: 10_000, jitter: false) == 200
      assert Backoff.next_delay(3, base: 100, cap: 10_000, jitter: false) == 400
      assert Backoff.next_delay(4, base: 100, cap: 10_000, jitter: false) == 800
    end

    test "caps at max" do
      assert Backoff.next_delay(10, base: 100, cap: 500, jitter: false) == 500
      assert Backoff.next_delay(20, base: 100, cap: 500, jitter: false) == 500
    end

    test "defaults: base=500, cap=30_000" do
      d = Backoff.next_delay(1, jitter: false)
      assert d == 500
    end
  end

  describe "next_delay/2 — with full jitter" do
    test "returns value within [0, capped]" do
      for attempt <- 1..10 do
        d = Backoff.next_delay(attempt, base: 100, cap: 5_000, jitter: true)
        assert d >= 0
        assert d <= 5_000
      end
    end

    test "produces varying values across calls" do
      delays =
        for _attempt <- 1..50, do: Backoff.next_delay(3, base: 500, cap: 10_000, jitter: true)

      assert Enum.count(Enum.uniq(delays)) > 1
    end
  end

  describe "sequence/2" do
    test "returns list of delays in order" do
      seq = Backoff.sequence(5, base: 100, cap: 10_000, jitter: false)
      assert seq == [100, 200, 400, 800, 1_600]
    end

    test "all values respect the cap" do
      seq = Backoff.sequence(10, base: 1_000, cap: 3_000, jitter: false)
      assert Enum.all?(seq, &(&1 <= 3_000))
    end

    test "returns empty list for count 0" do
      assert Backoff.sequence(0) == []
    end
  end

  describe "sleep/2" do
    test "returns the delay used" do
      delay = Backoff.sleep(1, base: 1, cap: 5, jitter: false)
      assert delay == 1
    end
  end
end
