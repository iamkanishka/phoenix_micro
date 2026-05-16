defmodule PhoenixMicro.Outbox do
  @moduledoc """
  Transactional outbox pattern for guaranteed message delivery.

  ## The problem this solves

  A naive `Repo.insert(order) + PhoenixMicro.publish(event)` has a race:

  ```
  1. Repo.insert(order)   ← succeeds
  2. publish(event)       ← crashes / network error
     └── event is LOST
  ```

  The outbox pattern eliminates the race by writing the event *inside the
  same database transaction* as the business record, then relaying it to
  the broker in a separate background process:

  ```
  Transaction:
    1. Repo.insert(order)
    2. Outbox.enqueue(event)   ← writes to outbox_messages table
    COMMIT ──────────────────── both succeed or both roll back

  Relay (background):
    3. Poll outbox_messages WHERE relayed_at IS NULL
    4. publish(event) to broker
    5. UPDATE outbox_messages SET relayed_at = now()
  ```

  ## Setup

  ### 1. Generate and run the migration

      mix phx.gen.migration create_outbox_messages

  Add to the migration:

      def change do
        create table(:outbox_messages, primary_key: false) do
          add :id,           :uuid,    primary_key: true, default: fragment("gen_random_uuid()")
          add :topic,        :string,  null: false
          add :payload,      :map,     null: false
          add :headers,      :map,     default: %{}
          add :attempt,      :integer, default: 1
          add :relayed_at,   :utc_datetime_usec
          add :failed_at,    :utc_datetime_usec
          add :last_error,   :string
          timestamps(type: :utc_datetime_usec)
        end

        create index(:outbox_messages, [:relayed_at, :inserted_at])
        create index(:outbox_messages, [:failed_at])
      end

  ### 2. Configure

      config :phoenix_micro,
        outbox: [
          repo: MyApp.Repo,
          poll_interval_ms: 1_000,
          batch_size: 100,
          max_attempts: 5
        ]

  ### 3. Add the relay to your supervision tree

      children = [
        MyApp.Repo,
        PhoenixMicro.Outbox.Relay
      ]

  ### 4. Use inside transactions

      Repo.transaction(fn ->
        order = Repo.insert!(Order.changeset(%Order{}, params))
        Outbox.enqueue("orders.placed", %{order_id: order.id})
      end)

  ## Guarantees

  - **At-least-once delivery** — if the relay crashes mid-flight, the
    message is still in the database and will be retried.
  - **No phantom events** — if the outer transaction rolls back, the
    outbox row rolls back too.
  - **Idempotent** — the relay sets `relayed_at` only after a successful
    broker publish, so duplicate delivery is bounded.

  ## Deduplication

  Consumers should handle duplicates using `PhoenixMicro.Middleware.Idempotency`
  or their own deduplication logic. The message `id` is stable across retries.
  """

  alias PhoenixMicro.{Config, Message}

  @doc """
  Enqueues a message in the outbox table.
  Must be called inside an Ecto `Repo.transaction/1` block.

  Returns `{:ok, outbox_record}` or `{:error, changeset}`.
  """
  @spec enqueue(String.t(), term(), keyword()) :: {:ok, map()} | {:error, term()}
  def enqueue(topic, payload, opts \\ []) do
    repo = outbox_repo!()

    record = %{
      id: Keyword.get(opts, :id, Message.generate_id()),
      topic: topic,
      payload: payload,
      headers: Keyword.get(opts, :headers, %{}),
      attempt: 1,
      relayed_at: nil,
      failed_at: nil,
      last_error: nil
    }

    case repo.insert(outbox_schema().__struct__(record), returning: true) do
      {:ok, row} -> {:ok, row}
      {:error, changeset} -> {:error, changeset}
    end
  rescue
    e -> {:error, e}
  end

  @doc """
  Enqueues a message, raising on failure.
  """
  @spec enqueue!(String.t(), term(), keyword()) :: map()
  def enqueue!(topic, payload, opts \\ []) do
    case enqueue(topic, payload, opts) do
      {:ok, row} -> row
      {:error, reason} -> raise "Outbox.enqueue! failed: #{inspect(reason)}"
    end
  end

  @doc false
  def outbox_repo! do
    config = Config.get(:outbox, [])

    Keyword.get(config, :repo) ||
      raise "PhoenixMicro.Outbox requires `config :phoenix_micro, outbox: [repo: MyApp.Repo]`"
  end

  @doc false
  def outbox_config, do: Config.get(:outbox, [])

  @doc false
  def outbox_schema do
    Keyword.get(outbox_config(), :schema, PhoenixMicro.Outbox.Message)
  end
end

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Default Ecto schema for outbox_messages — only compiled when Ecto is loaded
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Outbox.Message do
  @moduledoc """
  Ecto schema for the `outbox_messages` table.

  Only available when `ecto` and `ecto_sql` are in your application's deps.
  Add them to use the transactional outbox feature:

      {:ecto_sql, "~> 3.11"}
      {:postgrex, ">= 0.0.0"}

  You can replace this schema by setting:

      config :phoenix_micro, outbox: [schema: MyApp.OutboxMessage]
  """

  # When Ecto is available, delegate to it at runtime via apply/3.
  # We cannot use `use Ecto.Schema` inside `if` because `use` is a macro
  # that expands at compile time regardless of the runtime condition.
  # Instead, define a plain struct that mirrors the schema columns.
  defstruct [
    :id,
    :topic,
    :payload,
    :headers,
    :attempt,
    :relayed_at,
    :failed_at,
    :last_error,
    :inserted_at,
    :updated_at
  ]

  @doc "Returns true when Ecto is available in the host application."
  @spec ecto_available?() :: boolean()
  def ecto_available?, do: Code.ensure_loaded?(Ecto.Schema)

  @doc false
  def changeset(record, attrs) do
    if Code.ensure_loaded?(Ecto.Changeset) do
      record
      |> ecto_cast(attrs)
      |> ecto_validate_required([:topic, :payload])
    else
      {:error, :ecto_not_available}
    end
  end

  defp ecto_cast(record, attrs) do
    apply(Ecto.Changeset, :cast, [
      record,
      attrs,
      [:topic, :payload, :headers, :attempt, :relayed_at, :failed_at, :last_error]
    ])
  end

  defp ecto_validate_required(changeset, fields) do
    apply(Ecto.Changeset, :validate_required, [changeset, fields])
  end

  @doc """
  Returns the Ecto schema source table name.
  Used by the Relay when building queries.
  """
  @spec __schema__(:source) :: String.t()
  def __schema__(:source), do: "outbox_messages"
end

# ---------------------------------------------------------------------------
# Relay — background process that polls and publishes
# ---------------------------------------------------------------------------

defmodule PhoenixMicro.Outbox.Relay do
  @moduledoc """
  GenServer that polls the `outbox_messages` table and relays undelivered
  messages to the configured transport.

  Requires `ecto_sql` in your application's deps:

      {:ecto_sql, "~> 3.11"}

  ## Behaviour

  Every `:poll_interval_ms` (default 1000ms) the relay:

  1. Selects up to `:batch_size` rows where `relayed_at IS NULL`
     and `attempt <= max_attempts`, ordered by `inserted_at ASC`.
  2. For each row, calls `PhoenixMicro.publish_sync/3`.
  3. On success: marks `relayed_at = now()`.
  4. On failure: increments `attempt`, sets `last_error`.
     Once `attempt > max_attempts`, sets `failed_at = now()` and gives up.
  """

  use GenServer

  require Logger

  alias PhoenixMicro.{Outbox, Telemetry}

  @default_poll_interval_ms 1_000
  @default_batch_size 100
  @default_max_attempts 5

  defstruct [:repo, :poll_interval_ms, :batch_size, :max_attempts, :schema]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  # ---------------------------------------------------------------------------
  # GenServer
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    if Code.ensure_loaded?(Ecto.Query) do
      config = Keyword.merge(Outbox.outbox_config(), opts)
      repo = Keyword.get(config, :repo)

      unless repo do
        raise "PhoenixMicro.Outbox.Relay requires `config :phoenix_micro, outbox: [repo: MyApp.Repo]`"
      end

      state = %__MODULE__{
        repo: repo,
        schema: Keyword.get(config, :schema, PhoenixMicro.Outbox.Message),
        poll_interval_ms: Keyword.get(config, :poll_interval_ms, @default_poll_interval_ms),
        batch_size: Keyword.get(config, :batch_size, @default_batch_size),
        max_attempts: Keyword.get(config, :max_attempts, @default_max_attempts)
      }

      schedule_poll(state.poll_interval_ms)
      Logger.info("[Outbox.Relay] Started, polling every #{state.poll_interval_ms}ms")
      {:ok, state}
    else
      Logger.warning(
        "[Outbox.Relay] ecto_sql not loaded — relay disabled. " <>
          "Add {:ecto_sql, \"~> 3.11\"} to your application's deps."
      )

      :ignore
    end
  end

  @impl GenServer
  def handle_info(:poll, state) do
    relay_pending(state)
    schedule_poll(state.poll_interval_ms)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(_msg, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Relay logic — all Ecto calls are via apply/3 to avoid compile-time errors
  # ---------------------------------------------------------------------------

  defp relay_pending(state) do
    pending = fetch_pending(state)

    if pending != [] do
      Logger.debug("[Outbox.Relay] Relaying #{length(pending)} pending messages")
    end

    Enum.each(pending, &relay_one(&1, state))
  end

  defp fetch_pending(state) do
    # All Ecto.Query macros (from, where, order_by, limit, field) expand at
    # compile time and cannot be used here. Raw SQL is the only approach that
    # is truly runtime-only and works regardless of whether Ecto is a dep.
    table = state.schema.__schema__(:source)

    sql = """
    SELECT * FROM #{table}
    WHERE relayed_at IS NULL AND failed_at IS NULL
    ORDER BY inserted_at ASC
    LIMIT $1
    """

    result = apply(state.repo, :query!, [sql, [state.batch_size]])
    rows_to_structs(result, state.schema)
  rescue
    _e -> []
  end

  defp rows_to_structs(%{columns: cols, rows: rows}, schema) do
    col_atoms = Enum.map(cols, &String.to_existing_atom/1)

    Enum.map(rows, fn row ->
      fields = col_atoms |> Enum.zip(row) |> Map.new()
      struct(schema, fields)
    end)
  rescue
    _e -> []
  end

  defp relay_one(row, state) do
    start = System.monotonic_time()

    case PhoenixMicro.publish_sync(row.topic, row.payload,
           headers: row.headers || %{},
           correlation_id: to_string(row.id)
         ) do
      :ok ->
        duration = System.monotonic_time() - start
        mark_relayed(row, state)

        Telemetry.message_published(row.topic, %{
          transport: :outbox,
          duration: duration,
          outbox_id: row.id
        })

        Logger.debug("[Outbox.Relay] Relayed #{row.id} → #{row.topic}")

      {:error, reason} ->
        handle_relay_failure(row, reason, state)
    end
  end

  defp mark_relayed(row, state) do
    table = state.schema.__schema__(:source)
    sql = "UPDATE #{table} SET relayed_at = $1 WHERE id = $2"
    apply(state.repo, :query!, [sql, [DateTime.utc_now(), row.id]])
  rescue
    _e -> :ok
  end

  defp handle_relay_failure(row, reason, state) do
    new_attempt = row.attempt + 1
    error_msg = inspect(reason)

    Logger.warning(
      "[Outbox.Relay] Failed to relay #{row.id} (attempt #{row.attempt}): #{error_msg}"
    )

    table = state.schema.__schema__(:source)

    if new_attempt > state.max_attempts do
      Logger.error("[Outbox.Relay] Giving up on #{row.id} after #{row.attempt} attempts")

      sql = "UPDATE #{table} SET failed_at = $1, last_error = $2, attempt = $3 WHERE id = $4"
      apply(state.repo, :query!, [sql, [DateTime.utc_now(), error_msg, new_attempt, row.id]])

      Telemetry.message_failed(row.topic, %{reason: reason, final: true, outbox_id: row.id})
    else
      sql = "UPDATE #{table} SET attempt = $1, last_error = $2 WHERE id = $3"
      apply(state.repo, :query!, [sql, [new_attempt, error_msg, row.id]])
    end
  rescue
    _e -> :ok
  end

  defp schedule_poll(interval) do
    Process.send_after(self(), :poll, interval)
  end
end
