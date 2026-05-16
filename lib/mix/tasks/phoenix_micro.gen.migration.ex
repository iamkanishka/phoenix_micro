defmodule Mix.Tasks.PhoenixMicro.Gen.Migration do
  use Mix.Task

  @shortdoc "Generates the outbox_messages Ecto migration for PhoenixMicro"

  @moduledoc """
  Generates the `outbox_messages` table migration required by
  `PhoenixMicro.Outbox`.

  ## Usage

      mix phoenix_micro.gen.migration

  ## Options

      --repo MODULE     Ecto repo module (default: inferred from Mix project)
      --table NAME      Table name (default: outbox_messages)
      --no-index        Skip creating indexes

  ## What it generates

  A timestamped migration file in `priv/repo/migrations/` that creates:

  - `outbox_messages` table with UUID primary key
  - Columns: topic, payload (jsonb), headers (jsonb), attempt, relayed_at,
    failed_at, last_error, inserted_at, updated_at
  - Index on `(relayed_at, inserted_at)` for efficient relay polling
  - Index on `failed_at` for DLQ monitoring queries
  - Uses `gen_random_uuid()` for Postgres UUID generation
  """

  @switches [
    repo: :string,
    table: :string,
    no_index: :boolean
  ]

  @spec run([String.t()]) :: any()
  @impl Mix.Task
  def run(argv) do
    {opts, _args, _errors} = OptionParser.parse(argv, switches: @switches)

    table = Keyword.get(opts, :table, "outbox_messages")
    no_index = Keyword.get(opts, :no_index, false)
    repo = detect_repo(Keyword.get(opts, :repo))
    migrations_path = migrations_path(repo)

    timestamp = Calendar.strftime(DateTime.utc_now(), "%Y%m%d%H%M%S")
    filename = "#{timestamp}_create_#{table}.exs"
    full_path = Path.join(migrations_path, filename)

    module_name = migration_module_name(repo, table, timestamp)
    content = render_migration(module_name, table, no_index)

    File.mkdir_p!(migrations_path)
    File.write!(full_path, content)

    IO.puts("""
      * create #{full_path}

    Run the migration with:

        mix ecto.migrate

    Then configure PhoenixMicro:

        config :phoenix_micro,
          outbox: [
            repo: #{repo},
            poll_interval_ms: 1_000,
            batch_size: 100,
            max_attempts: 5
          ]

    Add the relay to your supervision tree:

        children = [
          #{repo},
          PhoenixMicro.Outbox.Relay,
          # ...
        ]
    """)
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp render_migration(module_name, table, no_index) do
    index_block =
      unless no_index do
        """
            # Relay poller: fetch oldest un-relayed messages efficiently
            create index(:#{table}, [:relayed_at, :inserted_at],
              where: "relayed_at IS NULL AND failed_at IS NULL",
              name: :#{table}_pending_idx
            )

            # DLQ monitoring: find messages that exhausted all retries
            create index(:#{table}, [:failed_at],
              where: "failed_at IS NOT NULL",
              name: :#{table}_failed_idx
            )
        """
      else
        ""
      end

    """
    defmodule #{module_name} do
      use Ecto.Migration

      def change do
        create table(:#{table}, primary_key: false) do
          add :id,          :binary_id, primary_key: true,
                            default: fragment("gen_random_uuid()")

          add :topic,       :string,  null: false
          add :payload,     :map,     null: false
          add :headers,     :map,     null: false, default: %{}
          add :attempt,     :integer, null: false, default: 1

          # Set when successfully relayed to the broker
          add :relayed_at,  :utc_datetime_usec

          # Set when max_attempts exhausted — message is dead
          add :failed_at,   :utc_datetime_usec
          add :last_error,  :text

          timestamps(type: :utc_datetime_usec)
        end

    #{index_block}
      end
    end
    """
  end

  defp detect_repo(nil) do
    # Try to infer from the Mix project's app name
    app = apply(Mix.Project, :config, [])[:app]
    module = app |> to_string() |> Macro.camelize()
    "#{module}.Repo"
  end

  defp detect_repo(repo), do: repo

  defp migrations_path(repo) do
    # Convert "MyApp.Repo" → "my_app/repo" for path building
    repo_path =
      repo
      |> String.replace(".", "/")
      |> Macro.underscore()

    # Standard Ecto convention: priv/<repo_underscored>/migrations
    repo_dir = repo_path |> Path.basename()
    "priv/#{repo_dir}/migrations"
  end

  defp migration_module_name(repo, table, timestamp) do
    # e.g. MyApp.Repo → MyApp.Repo.Migrations.CreateOutboxMessages20250101120000
    table_module = table |> Macro.camelize()
    "#{repo}.Migrations.Create#{table_module}#{timestamp}"
  end
end
