defmodule PhoenixMicro.Utils.WorkerPool do
  @moduledoc """
  A lightweight bounded worker pool built on `Task.Supervisor`.

  Wraps `Task.Supervisor` with a semaphore to enforce a maximum concurrency
  limit, preventing message handlers from spawning unbounded processes when
  the downstream is slow.

  ## Architecture

      WorkerPool (GenServer)
      ├── Task.Supervisor (owned)
      ├── semaphore: :counters ref (atomic, lock-free)
      └── pending queue: :queue (messages waiting for a slot)

  When all `max_concurrency` slots are taken:
  - New tasks are queued internally.
  - As workers complete, queued tasks are dispatched automatically.
  - If the queue exceeds `max_queue_size`, back-pressure is applied by
    returning `{:error, :overloaded}`.

  ## Usage

      # Start as part of a supervisor
      children = [
        {PhoenixMicro.Utils.WorkerPool, name: :payment_pool, max_concurrency: 10}
      ]

      # Submit work
      case WorkerPool.submit(:payment_pool, fn -> process_payment(msg) end) do
        {:ok, task} -> :ok
        {:error, :overloaded} -> nack_message(msg)
      end

      # Submit and wait for result
      {:ok, result} = WorkerPool.submit_await(:payment_pool, fn -> compute() end, 5_000)
  """

  use GenServer

  require Logger

  @default_max_concurrency 10
  @default_max_queue_size 1_000

  defstruct [
    :task_sup,
    # :counters ref — current active count
    :semaphore,
    :max_concurrency,
    :max_queue_size,
    pending: :queue.new(),
    pending_size: 0,
    active: 0
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def child_spec(opts) do
    name = Keyword.get(opts, :name, __MODULE__)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  @doc """
  Submits a function to the pool for async execution.
  Returns `{:ok, task}` or `{:error, :overloaded}` if the queue is full.
  """
  @spec submit(GenServer.name(), (-> term())) :: {:ok, Task.t()} | {:error, :overloaded}
  def submit(pool \\ __MODULE__, fun) when is_function(fun, 0) do
    GenServer.call(pool, {:submit, fun})
  end

  @doc """
  Submits a function and waits for its result.
  Returns `{:ok, result}` or `{:error, :timeout | :overloaded}`.
  """
  @spec submit_await(GenServer.name(), (-> term()), timeout()) ::
          {:ok, term()} | {:error, :timeout | :overloaded}
  def submit_await(pool \\ __MODULE__, fun, timeout \\ 5_000) do
    caller = self()
    ref = make_ref()

    wrapped = fn ->
      result = fun.()
      send(caller, {ref, result})
    end

    case submit(pool, wrapped) do
      {:ok, _task} ->
        receive do
          {^ref, result} -> {:ok, result}
        after
          timeout -> {:error, :timeout}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Returns current pool status: active workers, pending queue size."
  @spec status(GenServer.name()) :: %{
          active: integer(),
          pending: integer(),
          max_concurrency: integer()
        }
  def status(pool \\ __MODULE__) do
    GenServer.call(pool, :status)
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)
    max_queue_size = Keyword.get(opts, :max_queue_size, @default_max_queue_size)

    {:ok, task_sup} = Task.Supervisor.start_link()

    state = %__MODULE__{
      task_sup: task_sup,
      max_concurrency: max_concurrency,
      max_queue_size: max_queue_size,
      active: 0,
      pending: :queue.new(),
      pending_size: 0
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:submit, fun}, _from, state) do
    if state.active < state.max_concurrency do
      {task, new_state} = spawn_task(fun, state)
      {:reply, {:ok, task}, new_state}
    else
      if state.pending_size >= state.max_queue_size do
        Logger.warning("[WorkerPool] Queue full (#{state.max_queue_size}), rejecting task")
        {:reply, {:error, :overloaded}, state}
      else
        new_pending = :queue.in(fun, state.pending)
        new_state = %{state | pending: new_pending, pending_size: state.pending_size + 1}
        {:reply, {:ok, :queued}, new_state}
      end
    end
  end

  @impl GenServer
  def handle_call(:status, _from, state) do
    status = %{
      active: state.active,
      pending: state.pending_size,
      max_concurrency: state.max_concurrency
    }

    {:reply, status, state}
  end

  @impl GenServer
  def handle_info({ref, _result}, state) when is_reference(ref) do
    # Task completed normally — demonitor and free a slot
    Process.demonitor(ref, [:flush])
    new_state = %{state | active: state.active - 1}
    {:noreply, drain_queue(new_state)}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    if reason != :normal do
      Logger.warning("[WorkerPool] Worker exited: #{inspect(reason)}")
    end

    new_state = %{state | active: max(0, state.active - 1)}
    {:noreply, drain_queue(new_state)}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp spawn_task(fun, state) do
    task = Task.Supervisor.async_nolink(state.task_sup, fun)
    {task, %{state | active: state.active + 1}}
  end

  defp drain_queue(%{pending_size: 0} = state), do: state

  defp drain_queue(state) do
    if state.active < state.max_concurrency do
      case :queue.out(state.pending) do
        {{:value, fun}, new_pending} ->
          {_t, new_state} =
            spawn_task(fun, %{state | pending: new_pending, pending_size: state.pending_size - 1})

          drain_queue(new_state)

        {:empty, _queue} ->
          state
      end
    else
      state
    end
  end
end
