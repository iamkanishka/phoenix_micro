defmodule PhoenixMicro.Saga do
  @moduledoc """
  Compensation-based saga coordinator for distributed transactions.

  ## What is a saga?

  A saga is a sequence of steps where each step has a corresponding
  **compensation** (undo) action. If any step fails, all previously
  completed steps are compensated in reverse order:

  ```
  Step 1: Reserve inventory   ─── compensation: Release inventory
  Step 2: Charge payment      ─── compensation: Refund payment
  Step 3: Create shipment     ─── compensation: Cancel shipment
  Step 4: Send notification   ─── compensation: (none — fire-and-forget)
  ```

  If Step 3 fails:
  ```
  Compensation 2: Refund payment
  Compensation 1: Release inventory
  ```

  ## OTP model

  Each saga execution runs as an independent `GenServer` process owned
  by `PhoenixMicro.Saga.Supervisor`. This means:

  - Sagas survive transport failures (they are local processes).
  - Each saga has its own state and failure-isolation boundary.
  - The supervisor can restart failed sagas if needed.
  - You can query a saga's status by its ID at any time.

  ## Defining a saga

      defmodule MyApp.PlaceOrderSaga do
        use PhoenixMicro.Saga

        step :reserve_inventory,
          execute: &MyApp.Inventory.reserve/1,
          compensate: &MyApp.Inventory.release/1

        step :charge_payment,
          execute: &MyApp.Payments.charge/1,
          compensate: &MyApp.Payments.refund/1

        step :create_shipment,
          execute: &MyApp.Shipping.create/1,
          compensate: &MyApp.Shipping.cancel/1

        step :notify_customer,
          execute: &MyApp.Notifications.send_order_confirmation/1
          # no compensate — notifications are fire-and-forget
      end

  ## Running a saga

      # Start and await completion (synchronous)
      case PhoenixMicro.Saga.run(PlaceOrderSaga, %{order_id: "ord_123"}) do
        {:ok, final_context} -> handle_success(final_context)
        {:compensated, reason, context} -> handle_rollback(reason, context)
        {:error, reason} -> handle_fatal(reason)
      end

      # Start async — returns saga_id immediately
      {:ok, saga_id} = PhoenixMicro.Saga.start(PlaceOrderSaga, %{order_id: "ord_123"})

      # Check status later
      {:ok, %{status: :completed}} = PhoenixMicro.Saga.status(saga_id)

  ## Step function signatures

  Execute functions receive the current saga context map:

      def my_execute(%{order_id: order_id} = ctx) do
        case do_work(order_id) do
          {:ok, result} ->
            # Merge result into context for downstream steps
            {:ok, Map.put(ctx, :my_result, result)}

          {:error, reason} ->
            {:error, reason}
        end
      end

  Compensate functions receive the same context map at the time the
  step completed:

      def my_compensate(%{order_id: order_id, my_result: result}) do
        undo_work(order_id, result)
        :ok
      end

  ## Telemetry

  - `[:phoenix_micro, :saga, :started]`      — saga execution started
  - `[:phoenix_micro, :saga, :step_started]` — individual step starting
  - `[:phoenix_micro, :saga, :step_ok]`      — step completed successfully
  - `[:phoenix_micro, :saga, :step_failed]`  — step returned error
  - `[:phoenix_micro, :saga, :compensating]` — rollback started
  - `[:phoenix_micro, :saga, :completed]`    — all steps succeeded
  - `[:phoenix_micro, :saga, :compensated]`  — saga rolled back cleanly
  - `[:phoenix_micro, :saga, :fatal]`        — compensation itself failed
  """

  # ---------------------------------------------------------------------------
  # Macros (DSL)
  # ---------------------------------------------------------------------------

  defmacro __using__(_opts) do
    quote do
      import PhoenixMicro.Saga, only: [step: 2, step: 3]
      Module.register_attribute(__MODULE__, :pm_saga_steps, accumulate: true)
      @before_compile PhoenixMicro.Saga
    end
  end

  @doc """
  Defines a saga step.

  Options:
  - `:execute`    — required. `fn(context) :: {:ok, context} | {:error, reason}`
  - `:compensate` — optional. `fn(context) :: :ok | {:error, reason}`
  - `:timeout`    — milliseconds for this step (default: 30_000)
  - `:retries`    — number of retries on transient failure (default: 0)
  """
  defmacro step(name, opts) do
    quote do
      @pm_saga_steps {unquote(name), unquote(opts)}
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      @doc false
      def __saga_steps__ do
        # @pm_saga_steps accumulates in reverse
        Enum.reverse(@pm_saga_steps)
        |> Enum.map(fn {name, opts} ->
          %PhoenixMicro.Saga.Step{
            name: name,
            execute: Keyword.fetch!(opts, :execute),
            compensate: Keyword.get(opts, :compensate),
            timeout: Keyword.get(opts, :timeout, 30_000),
            retries: Keyword.get(opts, :retries, 0)
          }
        end)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Runs a saga synchronously, blocking until completion or rollback.
  Returns:
  - `{:ok, final_context}` — all steps completed successfully.
  - `{:compensated, reason, context}` — a step failed; compensations ran.
  - `{:error, :compensation_failed, details}` — compensation itself failed.
  """
  @spec run(module(), map(), keyword()) ::
          {:ok, map()}
          | {:compensated, term(), map()}
          | {:error, :compensation_failed, term()}
  def run(saga_module, initial_context, opts \\ []) do
    {:ok, saga_id} = start(saga_module, initial_context, opts)
    timeout = Keyword.get(opts, :timeout, :timer.minutes(5))

    wait_for_result(saga_id, timeout)
  end

  @doc """
  Starts a saga asynchronously. Returns `{:ok, saga_id}`.
  Use `status/1` to poll for completion.
  """
  @spec start(module(), map(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def start(saga_module, initial_context, opts \\ []) do
    saga_id = Keyword.get(opts, :id, PhoenixMicro.Message.generate_id())

    args = {saga_module, saga_id, initial_context, opts}

    case PhoenixMicro.Saga.Supervisor.start_saga(args) do
      {:ok, _pid} -> {:ok, saga_id}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns the current status of a saga.
  """
  @spec status(String.t()) :: {:ok, map()} | {:error, :not_found}
  def status(saga_id) do
    case find_saga(saga_id) do
      nil -> {:error, :not_found}
      pid -> {:ok, GenServer.call(pid, :status)}
    end
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp find_saga(saga_id) do
    case Registry.lookup(PhoenixMicro.Registry, {:saga, saga_id}) do
      [{pid, _value}] -> pid
      [] -> nil
    end
  end

  defp wait_for_result(saga_id, timeout) do
    caller = self()

    # Register a one-time listener
    {:ok, _reg} = Registry.register(PhoenixMicro.Registry, {:saga_listener, saga_id}, caller)

    receive do
      {:saga_result, ^saga_id, result} -> result
    after
      timeout ->
        {:error, :saga_timeout}
    end
  end
end

# ---------------------------------------------------------------------------
# Step struct
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Saga.Step do
  @moduledoc false

  @enforce_keys [:name, :execute]

  defstruct [
    :name,
    :execute,
    :compensate,
    timeout: 30_000,
    retries: 0
  ]

  @type t :: %__MODULE__{
          name: atom(),
          execute: (map() -> {:ok, map()} | {:error, term()}),
          compensate: (map() -> :ok | {:error, term()}) | nil,
          timeout: pos_integer(),
          retries: non_neg_integer()
        }
end

# ---------------------------------------------------------------------------
# Saga process
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Saga.Server do
  @moduledoc """
  GenServer that executes and coordinates a single saga run.
  """

  use GenServer, restart: :transient

  require Logger

  alias PhoenixMicro.Saga.Step

  defstruct [
    :saga_id,
    :saga_module,
    :steps,
    :context,
    :caller,
    :opts,
    completed_steps: [],
    status: :running,
    failure_reason: nil
  ]

  @spec start_link({module(), String.t(), map(), keyword()}) :: GenServer.on_start()
  def start_link({saga_module, saga_id, initial_context, opts}) do
    GenServer.start_link(
      __MODULE__,
      {saga_module, saga_id, initial_context, opts},
      name: via(saga_id)
    )
  end

  @impl GenServer
  def init({saga_module, saga_id, initial_context, opts}) do
    steps = saga_module.__saga_steps__()

    :telemetry.execute(
      [:phoenix_micro, :saga, :started],
      %{count: 1},
      %{saga_id: saga_id, saga_module: saga_module, steps: length(steps)}
    )

    Logger.info("[Saga] #{saga_module} #{saga_id} starting (#{length(steps)} steps)")

    state = %__MODULE__{
      saga_id: saga_id,
      saga_module: saga_module,
      steps: steps,
      context: initial_context,
      opts: opts
    }

    # Execute asynchronously so init/1 returns immediately
    send(self(), :execute)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:execute, state) do
    new_state = execute_steps(state)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    status = %{
      saga_id: state.saga_id,
      status: state.status,
      completed_steps: Enum.map(state.completed_steps, & &1.name),
      failure_reason: state.failure_reason,
      context: state.context
    }

    {:reply, status, state}
  end

  # ---------------------------------------------------------------------------
  # Execution engine
  # ---------------------------------------------------------------------------

  defp execute_steps(%{steps: []} = state) do
    Logger.info("[Saga] #{state.saga_module} #{state.saga_id} completed successfully")

    :telemetry.execute(
      [:phoenix_micro, :saga, :completed],
      %{count: 1, steps: length(state.completed_steps)},
      %{saga_id: state.saga_id, saga_module: state.saga_module}
    )

    notify_result(state, {:ok, state.context})
    %{state | status: :completed}
  end

  defp execute_steps(%{steps: [step | rest]} = state) do
    :telemetry.execute(
      [:phoenix_micro, :saga, :step_started],
      %{count: 1},
      %{saga_id: state.saga_id, step: step.name}
    )

    Logger.debug("[Saga] #{state.saga_id} executing step :#{step.name}")

    case run_step(step, state.context) do
      {:ok, new_context} ->
        :telemetry.execute(
          [:phoenix_micro, :saga, :step_ok],
          %{count: 1},
          %{saga_id: state.saga_id, step: step.name}
        )

        new_state = %{
          state
          | steps: rest,
            context: new_context,
            completed_steps: [step | state.completed_steps]
        }

        execute_steps(new_state)

      {:error, reason} ->
        :telemetry.execute(
          [:phoenix_micro, :saga, :step_failed],
          %{count: 1},
          %{saga_id: state.saga_id, step: step.name, reason: inspect(reason)}
        )

        Logger.warning("[Saga] #{state.saga_id} step :#{step.name} failed: #{inspect(reason)}")
        compensate_steps(state, reason)
    end
  end

  defp run_step(%Step{retries: retries} = step, context) do
    do_run_step(step, context, retries)
  end

  defp do_run_step(step, context, retries_left) do
    case step.execute.(context) do
      {:ok, new_context} when is_map(new_context) ->
        {:ok, new_context}

      :ok ->
        {:ok, context}

      {:error, _reason} when retries_left > 0 ->
        Logger.debug("[Saga] Retrying step :#{step.name}, #{retries_left} attempts left")
        do_run_step(step, context, retries_left - 1)

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_return, other}}
    end
  rescue
    e -> {:error, {:exception, e, __STACKTRACE__}}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  # ---------------------------------------------------------------------------
  # Compensation engine
  # ---------------------------------------------------------------------------

  defp compensate_steps(state, failure_reason) do
    Logger.warning("[Saga] #{state.saga_id} compensating #{length(state.completed_steps)} steps")

    :telemetry.execute(
      [:phoenix_micro, :saga, :compensating],
      %{count: 1, steps: length(state.completed_steps)},
      %{saga_id: state.saga_id, reason: inspect(failure_reason)}
    )

    case run_compensations(state.completed_steps, state.context) do
      :ok ->
        :telemetry.execute(
          [:phoenix_micro, :saga, :compensated],
          %{count: 1},
          %{saga_id: state.saga_id, reason: inspect(failure_reason)}
        )

        Logger.info("[Saga] #{state.saga_id} compensated successfully")
        notify_result(state, {:compensated, failure_reason, state.context})
        %{state | status: :compensated, failure_reason: failure_reason}

      {:error, comp_reason} ->
        :telemetry.execute(
          [:phoenix_micro, :saga, :fatal],
          %{count: 1},
          %{saga_id: state.saga_id, reason: inspect(comp_reason)}
        )

        Logger.error("[Saga] #{state.saga_id} FATAL — compensation failed: #{inspect(comp_reason)}")

        notify_result(state, {:error, :compensation_failed, comp_reason})
        %{state | status: :fatal, failure_reason: comp_reason}
    end
  end

  # Compensate in reverse order (most recent first)
  defp run_compensations([], _context), do: :ok

  defp run_compensations([step | rest], context) do
    case run_compensation(step, context) do
      :ok ->
        Logger.debug("[Saga] Compensated step :#{step.name}")
        run_compensations(rest, context)

      {:error, reason} ->
        Logger.error("[Saga] Compensation failed for :#{step.name}: #{inspect(reason)}")
        {:error, {step.name, reason}}
    end
  end

  defp run_compensation(%Step{compensate: nil}, _context), do: :ok

  defp run_compensation(%Step{compensate: fun} = step, context) do
    case fun.(context) do
      :ok -> :ok
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_compensation_return, step.name, other}}
    end
  rescue
    e -> {:error, {:compensation_exception, step.name, e}}
  end

  # ---------------------------------------------------------------------------
  # Notify caller (sync run/3 waits on this)
  # ---------------------------------------------------------------------------

  defp notify_result(state, result) do
    listeners = Registry.lookup(PhoenixMicro.Registry, {:saga_listener, state.saga_id})

    Enum.each(listeners, fn {_pid, caller_pid} ->
      send(caller_pid, {:saga_result, state.saga_id, result})
    end)
  end

  defp via(saga_id) do
    {:via, Registry, {PhoenixMicro.Registry, {:saga, saga_id}}}
  end
end

# ---------------------------------------------------------------------------
# Supervisor for saga processes
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Saga.Supervisor do
  @moduledoc """
  DynamicSupervisor that owns all running saga processes.
  Each saga runs in its own isolated `Saga.Server` process.
  """

  use DynamicSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @spec start_saga({module(), String.t(), map(), keyword()}) ::
          DynamicSupervisor.on_start_child()
  def start_saga(args) do
    spec = %{
      id: make_ref(),
      start: {PhoenixMicro.Saga.Server, :start_link, [args]},
      restart: :transient,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
