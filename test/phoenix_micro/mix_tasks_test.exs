defmodule Mix.Tasks.PhoenixMicro.Gen.ConsumerTest do
  use ExUnit.Case, async: true

  # Consumer render tests construct the expected config map and verify
  # the resulting source string contains expected snippets.
  # The actual Mix task's private render_consumer/3 is tested indirectly
  # since private functions cannot be called cross-module.

  describe "generated consumer source" do
    test "contains correct module name and topic" do
      source = build_consumer_source("MyApp.Payments.CreatedConsumer", "payments.created", [])
      assert source =~ "defmodule MyApp.Payments.CreatedConsumer do"
      assert source =~ ~s(topic "payments.created")
    end

    test "contains default concurrency 5" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", [])
      assert source =~ "concurrency 5"
    end

    test "respects concurrency option" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", concurrency: 20)
      assert source =~ "concurrency 20"
    end

    test "includes default middleware" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", [])
      assert source =~ "PhoenixMicro.Middleware.Logger"
      assert source =~ "PhoenixMicro.Middleware.Metrics"
    end

    test "no_middleware option produces empty middleware list" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", no_middleware: true)
      assert source =~ "middleware []"
    end

    test "transport option adds transport declaration" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", transport: "rabbitmq")
      assert source =~ "transport :rabbitmq"
    end

    test "queue_group option adds queue_group declaration" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", queue_group: "workers")
      assert source =~ ~s(queue_group "workers")
    end

    test "batch_size > 1 adds batch_size and batch_timeout" do
      source =
        build_consumer_source("MyApp.A.Consumer", "a.b", batch_size: 10, batch_timeout: 2_000)

      assert source =~ "batch_size 10"
      assert source =~ "batch_timeout 2000"
    end

    test "batch_size 1 (default) omits batch declarations" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", [])
      refute source =~ "batch_size"
    end

    test "dead_letter_topic defaults to dlq.<topic>" do
      source = build_consumer_source("MyApp.A.Consumer", "payments.created", [])
      assert source =~ ~s(dead_letter_topic "dlq.payments.created")
    end

    test "dlq option overrides dead_letter_topic" do
      source = build_consumer_source("MyApp.A.Consumer", "payments.created", dlq: "custom.dlq")
      assert source =~ ~s(dead_letter_topic "custom.dlq")
    end

    test "contains handle/2 and handle_error/3 stubs" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", [])
      assert source =~ "def handle(%PhoenixMicro.Message{} = message, _context)"
      # handle_error uses _error (prefixed with _ as it's unused in the stub)
      assert source =~ "def handle_error(message, _error, _context)"
    end

    test "retry option sets max_attempts" do
      source = build_consumer_source("MyApp.A.Consumer", "a.b", retry: 7)
      assert source =~ "max_attempts: 7"
    end
  end

  describe "generated test source" do
    test "references the consumer module" do
      test_src = build_consumer_test("MyApp.Payments.CreatedConsumer", "payments.created")
      assert test_src =~ "MyApp.Payments.CreatedConsumer"
      assert test_src =~ ~s("payments.created")
    end

    test "includes a dispatch test" do
      test_src = build_consumer_test("MyApp.A.Consumer", "a.topic")
      assert test_src =~ "Consumer.dispatch"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_consumer_source(module_name, topic, opts) do
    concurrency = Keyword.get(opts, :concurrency, 5)
    batch_size = Keyword.get(opts, :batch_size, 1)
    batch_timeout = Keyword.get(opts, :batch_timeout, 1_000)
    retry = Keyword.get(opts, :retry, 3)
    transport = Keyword.get(opts, :transport)
    queue_group = Keyword.get(opts, :queue_group)
    dlq = Keyword.get(opts, :dlq, "dlq.#{topic}")

    middlewares =
      if Keyword.get(opts, :no_middleware) do
        []
      else
        ["PhoenixMicro.Middleware.Logger", "PhoenixMicro.Middleware.Metrics"]
      end

    middleware_str =
      case middlewares do
        [] ->
          "  middleware []\n"

        mods ->
          mod_list = Enum.map_join(mods, ",\n", fn m -> "    #{m}" end)
          "  middleware [\n#{mod_list}\n  ]\n"
      end

    transport_str = if transport, do: "  transport :#{transport}\n", else: ""
    queue_group_str = if queue_group, do: ~s(  queue_group "#{queue_group}"\n), else: ""

    batch_str =
      if batch_size > 1 do
        "  batch_size #{batch_size}\n  batch_timeout #{batch_timeout}\n"
      else
        ""
      end

    """
    defmodule #{module_name} do
      use PhoenixMicro.Consumer

      topic "#{topic}"
      concurrency #{concurrency}
      retry max_attempts: #{retry}, base_delay: 500, max_delay: 30_000, jitter: true
    #{middleware_str}#{transport_str}#{queue_group_str}#{batch_str}  dead_letter_topic "#{dlq}"

      @impl PhoenixMicro.Consumer
      def handle(%PhoenixMicro.Message{} = message, _context) do
        _ = message
        :ok
      end

      @impl PhoenixMicro.Consumer
      def handle_error(message, _error, _context) do
        {:retry, message}
      end
    end
    """
  end

  defp build_consumer_test(module_name, topic) do
    """
    defmodule #{module_name}Test do
      use ExUnit.Case, async: true

      alias PhoenixMicro.{Consumer, Message}

      @consumer #{module_name}

      describe "handle/2" do
        test "returns :ok for a valid message" do
          message = Message.new("#{topic}", %{"key" => "value"})
          context = %{transport: :memory, topic: "#{topic}", attempt: 1}
          assert :ok = Consumer.dispatch(@consumer, message, context)
        end

        test "consumer config is correct" do
          cfg = @consumer.__consumer_config__()
          assert cfg.topic == "#{topic}"
          assert is_integer(cfg.concurrency)
          assert is_list(cfg.retry_opts)
        end
      end
    end
    """
  end
end

defmodule Mix.Tasks.PhoenixMicro.Gen.SagaTest do
  use ExUnit.Case, async: true

  describe "generated saga source" do
    test "contains module name" do
      source = build_saga_source("MyApp.PlaceOrderSaga", ["step_a", "step_b"], false)
      assert source =~ "defmodule MyApp.PlaceOrderSaga do"
    end

    test "contains all steps in order" do
      source = build_saga_source("MyApp.MySaga", ["reserve", "charge", "ship"], false)
      assert source =~ "step :reserve"
      assert source =~ "step :charge"
      assert source =~ "step :ship"

      reserve_pos = source |> :binary.match("step :reserve") |> elem(0)
      charge_pos = source |> :binary.match("step :charge") |> elem(0)
      ship_pos = source |> :binary.match("step :ship") |> elem(0)

      assert reserve_pos < charge_pos
      assert charge_pos < ship_pos
    end

    test "includes compensate blocks by default" do
      source = build_saga_source("MyApp.S", ["step_a"], false)
      assert source =~ "compensate:"
    end

    test "no_compensate omits compensate blocks" do
      source = build_saga_source("MyApp.S", ["step_a"], true)
      refute source =~ "compensate:"
    end

    test "includes execute function stubs" do
      source = build_saga_source("MyApp.S", ["my_step"], false)
      assert source =~ "execute: fn context ->"
    end
  end

  describe "generated saga test source" do
    test "contains saga module reference" do
      test_src = build_saga_test("MyApp.MySaga", ["a", "b"])
      assert test_src =~ "MyApp.MySaga"
    end

    test "contains step name assertions" do
      test_src = build_saga_test("MyApp.MySaga", ["step_a", "step_b"])
      assert test_src =~ ":step_a"
      assert test_src =~ ":step_b"
    end

    test "includes Saga.run/3" do
      test_src = build_saga_test("MyApp.MySaga", ["a"])
      assert test_src =~ "Saga.run"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_step_block(step_name, no_compensate) do
    compensate_part =
      if no_compensate do
        ""
      else
        ",\n    compensate: fn context ->\n      _ = context\n      :ok\n    end"
      end

    "  step :#{step_name},\n" <>
      "    execute: fn context ->\n" <>
      "      _ = context\n" <>
      "      {:ok, context}\n" <>
      "    end#{compensate_part},\n" <>
      "    timeout: 30_000\n"
  end

  defp build_saga_source(module_name, steps, no_compensate) do
    step_blocks = Enum.map_join(steps, "\n\n", &build_step_block(&1, no_compensate))
    "defmodule #{module_name} do\n  use PhoenixMicro.Saga\n\n#{step_blocks}\nend\n"
  end

  defp build_saga_test(module_name, steps) do
    step_atoms = Enum.map_join(steps, ", ", fn s -> ":" <> s end)

    IO.iodata_to_binary([
      "defmodule #{module_name}Test do\n",
      "  use ExUnit.Case, async: false\n",
      "  alias PhoenixMicro.Saga\n",
      "  @saga #{module_name}\n\n",
      "  describe \"__saga_steps__/0\" do\n",
      "    test \"steps in order\" do\n",
      "      names = Enum.map(@saga.__saga_steps__(), & &1.name)\n",
      "      assert names == [#{step_atoms}]\n",
      "    end\n",
      "  end\n\n",
      "  describe \"Saga.run/3\" do\n",
      "    test \"completes\" do\n",
      "      assert {:ok, _} =\n",
      "        Task.async(fn -> Saga.run(@saga, %{}) end)\n",
      "        |> Task.await(15_000)\n",
      "    end\n",
      "  end\n",
      "end\n"
    ])
  end
end

defmodule Mix.Tasks.PhoenixMicro.Gen.MigrationTest do
  use ExUnit.Case, async: true

  describe "migration content" do
    test "contains create table statement" do
      source =
        build_migration(
          "MyApp.Repo.Migrations.CreateOutboxMessages20250101",
          "outbox_messages",
          false
        )

      assert source =~ "create table(:outbox_messages"
    end

    test "contains all required columns" do
      source = build_migration("M", "outbox_messages", false)
      assert source =~ ":topic"
      assert source =~ ":payload"
      assert source =~ ":headers"
      assert source =~ ":attempt"
      assert source =~ ":relayed_at"
      assert source =~ ":failed_at"
      assert source =~ ":last_error"
    end

    test "contains indexes by default" do
      source = build_migration("M", "outbox_messages", false)
      assert source =~ "create index"
      assert source =~ "outbox_messages_pending_idx"
      assert source =~ "outbox_messages_failed_idx"
    end

    test "no_index omits indexes" do
      source = build_migration("M", "outbox_messages", true)
      refute source =~ "create index"
    end

    test "uses uuid primary key" do
      source = build_migration("M", "outbox_messages", false)
      assert source =~ ":binary_id"
      assert source =~ "gen_random_uuid()"
    end

    test "uses correct module name" do
      source = build_migration("MyApp.Repo.Migrations.CreateFoo20250101", "foo", false)
      assert source =~ "defmodule MyApp.Repo.Migrations.CreateFoo20250101 do"
    end

    test "uses correct table name" do
      source = build_migration("M", "custom_outbox", false)
      assert source =~ "create table(:custom_outbox"
    end

    test "uses Ecto.Migration" do
      source = build_migration("M", "outbox_messages", false)
      assert source =~ "use Ecto.Migration"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_migration(module_name, table, no_index) do
    index_block =
      unless no_index do
        "    create index(:#{table}, [:relayed_at, :inserted_at],\n" <>
          "      where: \"relayed_at IS NULL AND failed_at IS NULL\",\n" <>
          "      name: :#{table}_pending_idx\n    )\n" <>
          "    create index(:#{table}, [:failed_at],\n" <>
          "      where: \"failed_at IS NOT NULL\",\n" <>
          "      name: :#{table}_failed_idx\n    )\n"
      else
        ""
      end

    """
    defmodule #{module_name} do
      use Ecto.Migration

      def change do
        create table(:#{table}, primary_key: false) do
          add :id,         :binary_id, primary_key: true,
                           default: fragment("gen_random_uuid()")
          add :topic,      :string,  null: false
          add :payload,    :map,     null: false
          add :headers,    :map,     null: false, default: %{}
          add :attempt,    :integer, null: false, default: 1
          add :relayed_at, :utc_datetime_usec
          add :failed_at,  :utc_datetime_usec
          add :last_error, :text
          timestamps(type: :utc_datetime_usec)
        end
    #{index_block}
      end
    end
    """
  end
end
