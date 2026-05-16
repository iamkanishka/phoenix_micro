defmodule PhoenixMicro.Utils.Backoff do
  @moduledoc """
  Shared exponential backoff with optional full jitter.

  Used by `Consumer.RetryScheduler`, `Transport.RabbitMQ`, `Transport.Kafka`,
  `Transport.NATS`, and `Transport.RedisStreams` for reconnect delays.

  ## Algorithm

  Full jitter (default, recommended for distributed systems):

      delay = random_between(0, min(cap, base * 2^attempt))

  Decorrelated jitter (alternative — avoids thundering herd even better):

      delay = random_between(base, min(cap, last_delay * 3))

  No jitter (deterministic — useful in tests):

      delay = min(cap, base * 2^attempt)

  ## Usage

      iex> PhoenixMicro.Utils.Backoff.next_delay(3, base: 500, cap: 30_000)
      # returns something between 0 and 4000ms (full jitter)

      iex> PhoenixMicro.Utils.Backoff.sequence(5, base: 100, cap: 5_000, jitter: false)
      [100, 200, 400, 800, 1600]
  """

  @type opts :: [
          base: pos_integer(),
          cap: pos_integer(),
          jitter: boolean() | :decorrelated,
          multiplier: number()
        ]

  @doc """
  Returns the next backoff delay in milliseconds for the given attempt number.

  `attempt` is 1-indexed (first retry = 1).

  ## Options

  - `:base` — base delay in ms (default: 500)
  - `:cap` — maximum delay in ms (default: 30_000)
  - `:jitter` — `true` for full jitter, `false` for none (default: `true`)
  - `:multiplier` — exponential factor (default: 2)
  """
  @spec next_delay(pos_integer(), opts()) :: non_neg_integer()
  def next_delay(attempt, opts \\ []) do
    base = Keyword.get(opts, :base, 500)
    cap = Keyword.get(opts, :cap, 30_000)
    jitter = Keyword.get(opts, :jitter, true)
    multiplier = Keyword.get(opts, :multiplier, 2)

    exponential = trunc(base * :math.pow(multiplier, attempt - 1))
    capped = min(exponential, cap)

    case jitter do
      false -> capped
      true -> :rand.uniform(max(capped, 1))
      :decorrelated -> decorrelated_jitter(base, capped)
      _jitter -> capped
    end
  end

  @doc """
  Returns a list of `count` delay values — useful for testing retry sequences.

  ## Example

      iex> PhoenixMicro.Utils.Backoff.sequence(4, base: 100, cap: 1_000, jitter: false)
      [100, 200, 400, 800]
  """
  @spec sequence(non_neg_integer(), opts()) :: [non_neg_integer()]
  def sequence(count, opts \\ [])
  def sequence(0, _opts), do: []

  def sequence(count, opts) when count > 0 do
    for attempt <- 1..count, do: next_delay(attempt, opts)
  end

  @doc """
  Sleeps for the calculated backoff delay.
  Returns the delay used (useful for logging).
  """
  @spec sleep(pos_integer(), opts()) :: non_neg_integer()
  def sleep(attempt, opts \\ []) do
    delay = next_delay(attempt, opts)
    Process.sleep(delay)
    delay
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp decorrelated_jitter(base, capped) do
    # Decorrelated: random between base and min(cap, last * 3)
    # Since we don't track last, approximate with capped as upper bound
    lower = base
    upper = max(lower + 1, min(capped, capped * 3))
    lower + :rand.uniform(upper - lower)
  end
end
