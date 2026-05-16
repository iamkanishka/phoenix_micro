defmodule PhoenixMicro.OutboxTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.{Message, Outbox}

  # ---------------------------------------------------------------------------
  # Outbox.Message schema
  # ---------------------------------------------------------------------------

  describe "Outbox.Message schema" do
    test "has expected fields" do
      fields = PhoenixMicro.Outbox.Message.__schema__(:fields)

      assert :topic in fields
      assert :payload in fields
      assert :headers in fields
      assert :attempt in fields
      assert :relayed_at in fields
      assert :failed_at in fields
      assert :last_error in fields
    end

    test "primary key is :id (binary_id)" do
      assert PhoenixMicro.Outbox.Message.__schema__(:primary_key) == [:id]
    end
  end

  # ---------------------------------------------------------------------------
  # Outbox.outbox_schema/0
  # ---------------------------------------------------------------------------

  describe "outbox_schema/0" do
    test "returns default schema when not configured" do
      Application.delete_env(:phoenix_micro, :outbox)
      assert Outbox.outbox_schema() == PhoenixMicro.Outbox.Message
    end

    test "returns custom schema when configured" do
      Application.put_env(:phoenix_micro, :outbox, schema: MyApp.CustomOutbox)
      assert Outbox.outbox_schema() == MyApp.CustomOutbox
      Application.delete_env(:phoenix_micro, :outbox)
    end
  end

  # ---------------------------------------------------------------------------
  # Outbox.outbox_repo!/0
  # ---------------------------------------------------------------------------

  describe "outbox_repo!/0" do
    test "raises when no repo configured" do
      Application.delete_env(:phoenix_micro, :outbox)

      assert_raise RuntimeError, ~r/outbox: \[repo:/, fn ->
        Outbox.outbox_repo!()
      end
    end

    test "returns configured repo" do
      Application.put_env(:phoenix_micro, :outbox, repo: MyApp.Repo)
      assert Outbox.outbox_repo!() == MyApp.Repo
      Application.delete_env(:phoenix_micro, :outbox)
    end
  end

  # ---------------------------------------------------------------------------
  # Outbox.enqueue/3 — without Ecto (tests config validation path)
  # ---------------------------------------------------------------------------

  describe "enqueue/3 without repo configured" do
    test "returns {:error, _} when no repo is set" do
      Application.delete_env(:phoenix_micro, :outbox)

      result = Outbox.enqueue("payments.created", %{amount: 100})
      assert {:error, _reason} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Relay configuration
  # ---------------------------------------------------------------------------

  describe "Relay config defaults" do
    test "child_spec has expected shape" do
      spec = PhoenixMicro.Outbox.Relay.child_spec([])

      assert spec.id == PhoenixMicro.Outbox.Relay
      assert spec.restart == :permanent
      assert spec.type == :worker
    end
  end

  # ---------------------------------------------------------------------------
  # Message ID generation (used by outbox)
  # ---------------------------------------------------------------------------

  describe "Message.generate_id/0 for outbox IDs" do
    test "generates unique IDs across many calls" do
      ids = for _i <- 1..500, do: Message.generate_id()
      assert Enum.count(Enum.uniq(ids)) == 500
    end

    test "generated ID is a valid UUID v4" do
      id = Message.generate_id()

      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               id
             )
    end
  end

  # ---------------------------------------------------------------------------
  # Outbox pattern documentation tests
  # ---------------------------------------------------------------------------

  describe "Outbox pattern invariants" do
    test "outbox_config returns keyword list" do
      Application.put_env(:phoenix_micro, :outbox,
        repo: MyApp.Repo,
        poll_interval_ms: 500,
        batch_size: 50
      )

      cfg = Outbox.outbox_config()
      assert Keyword.keyword?(cfg)
      assert cfg[:poll_interval_ms] == 500
      assert cfg[:batch_size] == 50

      Application.delete_env(:phoenix_micro, :outbox)
    end

    test "outbox_config returns empty list when not configured" do
      Application.delete_env(:phoenix_micro, :outbox)
      assert Outbox.outbox_config() == []
    end
  end
end
