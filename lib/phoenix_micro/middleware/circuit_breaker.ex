defmodule PhoenixMicro.Middleware.CircuitBreaker do
  @moduledoc """
  Circuit breaker middleware for consumer message handlers.

  Implements the classic three-state machine:

  ```
  CLOSED ──(failures >= threshold)──► OPEN
    ▲                                   │
    │                                 (reset_timeout elapses)
    │                                   ▼
    └──────(probe succeeds)────── HALF_OPEN
  ```

  ## States

  - **CLOSED** — Normal operation. All messages are processed.
    Failures are counted per fuse. When the failure count reaches
    `:threshold` within `:window_ms`, the breaker trips to OPEN.

  - **OPEN** — Circuit is tripped. Messages are rejected immediately
    with `{:error, :circuit_open}` — no downstream calls are made.
    After `:reset_timeout_ms`, the breaker transitions to HALF_OPEN.

  - **HALF_OPEN** — Probe state. The next single message is allowed
    through as a test. If it succeeds, the breaker resets to CLOSED.
    If it fails, the breaker returns to OPEN and the timeout restarts.

  ## Usage

  Add to a consumer's middleware list:

      defmodule MyApp.PaymentsConsumer do
        use PhoenixMicro.Consumer

        topic "payments.created"
        middleware [
          {PhoenixMicro.Middleware.CircuitBreaker,
           fuse: :payments_db,
           threshold: 5,
           window_ms: 10_000,
           reset_timeout_ms: 30_000}
        ]

        def handle(message, _ctx), do: MyApp.Repo.insert(...)
      end

  ## Fuse names

  The `:fuse` option names the circuit — multiple consumers can share
  a fuse (e.g. `fuse: :payments_db`) so that a downstream failure
  opens all of them simultaneously. Defaults to the topic name.

  ## Storage

  State is kept in an ETS table (`:phoenix_micro_circuit_breakers`) owned
  by `PhoenixMicro.Middleware.CircuitBreaker.Store`. This process must be
  started in your supervision tree, which `PhoenixMicro.Application` does
  automatically.

  ## Telemetry events

  - `[:phoenix_micro, :circuit_breaker, :tripped]`   — breaker opened
  - `[:phoenix_micro, :circuit_breaker, :reset]`      — breaker closed
  - `[:phoenix_micro, :circuit_breaker, :rejected]`   — message rejected (OPEN)
  - `[:phoenix_micro, :circuit_breaker, :probe]`      — probe sent (HALF_OPEN)
  """

  @behaviour PhoenixMicro.Middleware

  require Logger

  alias PhoenixMicro.Message
  alias PhoenixMicro.Middleware.CircuitBreaker.Store

  @default_threshold 5
  @default_window_ms 10_000
  @default_reset_timeout_ms 30_000

  # ---------------------------------------------------------------------------
  # Middleware callback
  # ---------------------------------------------------------------------------

  @impl PhoenixMicro.Middleware
  @spec call(Message.t(), PhoenixMicro.Middleware.next(), keyword()) ::
          :ok | {:error, term()}
  def call(%Message{} = message, next, opts \\ []) do
    fuse = Keyword.get(opts, :fuse, message.topic)
    threshold = Keyword.get(opts, :threshold, @default_threshold)
    window_ms = Keyword.get(opts, :window_ms, @default_window_ms)
    reset_timeout_ms = Keyword.get(opts, :reset_timeout_ms, @default_reset_timeout_ms)

    case Store.state(fuse) do
      :closed ->
        run_closed(message, next, fuse, threshold, window_ms)

      {:open, opened_at} ->
        handle_open_state(message, next, fuse, opened_at, reset_timeout_ms, threshold, window_ms)

      :half_open ->
        run_probe(message, next, fuse, threshold, window_ms)
    end
  end

  # ---------------------------------------------------------------------------
  # State handlers
  # ---------------------------------------------------------------------------

  defp run_closed(message, next, fuse, threshold, window_ms) do
    case next.(message) do
      :ok ->
        Store.record_success(fuse)
        :ok

      {:error, reason} = err ->
        count = Store.record_failure(fuse, window_ms)

        if count >= threshold do
          trip(fuse, reason)
        else
          Logger.debug(
            "[CircuitBreaker] #{fuse} failure #{count}/#{threshold}: #{inspect(reason)}"
          )
        end

        err
    end
  end

  defp handle_open_state(message, next, fuse, opened_at, reset_timeout_ms, threshold, window_ms) do
    now = System.monotonic_time(:millisecond)
    elapsed = now - opened_at

    if elapsed >= reset_timeout_ms do
      Logger.info("[CircuitBreaker] #{fuse} transitioning to HALF_OPEN after #{elapsed}ms")
      Store.set_half_open(fuse)
      run_probe(message, next, fuse, threshold, window_ms)
    else
      remaining = reset_timeout_ms - elapsed

      :telemetry.execute(
        [:phoenix_micro, :circuit_breaker, :rejected],
        %{count: 1},
        %{fuse: fuse, remaining_ms: remaining}
      )

      Logger.warning(
        "[CircuitBreaker] #{fuse} OPEN — rejecting message #{message.id}, resets in #{remaining}ms"
      )

      {:error, :circuit_open}
    end
  end

  defp run_probe(message, next, fuse, _threshold, _window_ms) do
    :telemetry.execute(
      [:phoenix_micro, :circuit_breaker, :probe],
      %{count: 1},
      %{fuse: fuse}
    )

    Logger.info("[CircuitBreaker] #{fuse} HALF_OPEN — sending probe message #{message.id}")

    case next.(message) do
      :ok ->
        Logger.info("[CircuitBreaker] #{fuse} probe succeeded — resetting to CLOSED")
        reset(fuse)
        :ok

      {:error, reason} = err ->
        Logger.warning(
          "[CircuitBreaker] #{fuse} probe failed — returning to OPEN: #{inspect(reason)}"
        )

        trip(fuse, reason)
        err
    end
  end

  defp trip(fuse, reason) do
    Store.set_open(fuse)

    :telemetry.execute(
      [:phoenix_micro, :circuit_breaker, :tripped],
      %{count: 1},
      %{fuse: fuse, reason: inspect(reason)}
    )

    Logger.error("[CircuitBreaker] #{fuse} TRIPPED — circuit is now OPEN")
  end

  defp reset(fuse) do
    Store.set_closed(fuse)

    :telemetry.execute(
      [:phoenix_micro, :circuit_breaker, :reset],
      %{count: 1},
      %{fuse: fuse}
    )
  end
end

# ---------------------------------------------------------------------------
# ETS-backed state store
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Middleware.CircuitBreaker.Store do
  @moduledoc """
  ETS-backed store for circuit breaker state.

  One entry per fuse name:

      {fuse_name, state, failure_window, opened_at}

  Where:
  - `state` is `:closed | {:open, monotonic_ms} | :half_open`
  - `failure_window` is a list of `{monotonic_ms, :failure}` tuples
    used to count failures within the sliding time window.

  The process owns the table so it survives crashes of consumers.
  """

  use GenServer

  @table :phoenix_micro_circuit_breakers

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current state for the given fuse: :closed | {:open, ms} | :half_open"
  @spec state(term()) :: :closed | {:open, integer()} | :half_open
  def state(fuse) do
    case :ets.lookup(@table, fuse) do
      [] -> :closed
      [{^fuse, :closed, _window, _opened_at}] -> :closed
      [{^fuse, {:open, opened_at}, _window, _opened_at}] -> {:open, opened_at}
      [{^fuse, :half_open, _window, _opened_at}] -> :half_open
    end
  end

  @doc "Records a success — clears the failure window."
  @spec record_success(term()) :: :ok
  def record_success(fuse) do
    :ets.insert(@table, {fuse, :closed, [], nil})
    :ok
  end

  @doc """
  Records a failure within the sliding window. Returns the current
  failure count within the window.
  """
  @spec record_failure(term(), pos_integer()) :: non_neg_integer()
  def record_failure(fuse, window_ms) do
    now = System.monotonic_time(:millisecond)
    cutoff = now - window_ms

    current =
      case :ets.lookup(@table, fuse) do
        [{^fuse, _state, window, _opened_at}] -> window
        [] -> []
      end

    pruned = Enum.filter(current, fn ts -> ts >= cutoff end)
    updated = [now | pruned]

    :ets.insert(@table, {fuse, :closed, updated, nil})
    length(updated)
  end

  @doc "Trips the breaker to OPEN."
  @spec set_open(term()) :: :ok
  def set_open(fuse) do
    now = System.monotonic_time(:millisecond)
    :ets.insert(@table, {fuse, {:open, now}, [], now})
    :ok
  end

  @doc "Transitions to HALF_OPEN for probe."
  @spec set_half_open(term()) :: :ok
  def set_half_open(fuse) do
    case :ets.lookup(@table, fuse) do
      [{^fuse, _state, window, opened_at}] ->
        :ets.insert(@table, {fuse, :half_open, window, opened_at})

      [] ->
        :ets.insert(@table, {fuse, :half_open, [], nil})
    end

    :ok
  end

  @doc "Resets the breaker to CLOSED."
  @spec set_closed(term()) :: :ok
  def set_closed(fuse) do
    :ets.insert(@table, {fuse, :closed, [], nil})
    :ok
  end

  @doc "Returns a snapshot of all fuse states (useful for health checks)."
  @spec all_states() :: [{term(), :closed | {:open, integer()} | :half_open}]
  def all_states do
    :ets.tab2list(@table)
    |> Enum.map(fn {fuse, state, _window, _opened_at} -> {fuse, state} end)
  end

  @doc "Resets all fuses to CLOSED. Useful in tests."
  @spec reset_all() :: :ok
  def reset_all do
    :ets.delete_all_objects(@table)
    :ok
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(_opts) do
    _table =
      :ets.new(@table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, %{}}
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end
end
