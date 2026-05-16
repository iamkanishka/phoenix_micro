defmodule PhoenixMicro.SchemaTest do
  use ExUnit.Case, async: false

  alias PhoenixMicro.Schema
  alias PhoenixMicro.Schema.Registry

  # ---------------------------------------------------------------------------
  # Test schema definitions
  # ---------------------------------------------------------------------------

  defmodule PaymentCreatedV1 do
    use PhoenixMicro.Schema

    schema_version(1)
    topic("test.payments.created")

    field(:payment_id, :string, required: true)
    field(:amount, :float, required: true)
    field(:currency, :string, required: true, default: "USD")
  end

  defmodule PaymentCreatedV2 do
    use PhoenixMicro.Schema

    schema_version(2)
    topic("test.payments.created.v2")

    field(:payment_id, :string, required: true)
    field(:amount_cents, :integer, required: true)
    field(:currency, :string, required: true, default: "USD")
    field(:metadata, :map, required: false)

    compatible_with([1])

    def migrate(1, payload) do
      amount = Map.get(payload, "amount") || Map.get(payload, :amount, 0)

      payload
      |> Map.delete("amount")
      |> Map.delete(:amount)
      |> Map.put("amount_cents", round(amount * 100))
    end
  end

  defmodule StrictSchema do
    use PhoenixMicro.Schema

    schema_version(1)
    topic("test.strict")

    field(:name, :string, required: true)
    field(:age, :integer, required: true)
    field(:active, :boolean, required: true)
    field(:score, :float, required: false)
    field(:tags, :list, required: false)
    field(:meta, :map, required: false)
    field(:label, :any, required: false)
  end

  defmodule NoTopicSchema do
    use PhoenixMicro.Schema
    schema_version(1)
    # deliberately no topic — should not register

    field(:value, :string, required: true)

    def topic, do: nil
  end

  # ---------------------------------------------------------------------------
  # __schema__ introspection
  # ---------------------------------------------------------------------------

  describe "schema introspection" do
    test "schema_version/0 returns declared version" do
      assert PaymentCreatedV1.schema_version() == 1
      assert PaymentCreatedV2.schema_version() == 2
    end

    test "topic/0 returns declared topic" do
      assert PaymentCreatedV1.topic() == "test.payments.created"
    end

    test "fields/0 returns field list in order" do
      [{:payment_id, :string, _opts1}, {:amount, :float, _opts2}, {:currency, :string, _opts3}] =
        PaymentCreatedV1.fields()
    end

    test "compatible_versions/0 returns declared list" do
      assert PaymentCreatedV2.compatible_versions() == [1]
      assert PaymentCreatedV1.compatible_versions() == []
    end
  end

  # ---------------------------------------------------------------------------
  # Validation — passing
  # ---------------------------------------------------------------------------

  describe "validate/1 — valid payloads" do
    test "accepts a valid payload with string keys" do
      payload = %{"payment_id" => "pay_123", "amount" => 9.99, "currency" => "GBP"}
      assert {:ok, validated} = PaymentCreatedV1.validate(payload)
      assert validated["payment_id"] == "pay_123"
    end

    test "accepts a valid payload with atom keys" do
      payload = %{payment_id: "pay_456", amount: 10.0, currency: "EUR"}
      assert {:ok, _validated} = PaymentCreatedV1.validate(payload)
    end

    test "applies default values for missing optional fields" do
      payload = %{"payment_id" => "pay_789", "amount" => 5.0}
      assert {:ok, validated} = PaymentCreatedV1.validate(payload)
      assert validated["currency"] == "USD"
    end

    test "accepts all types correctly" do
      payload = %{
        "name" => "Alice",
        "age" => 30,
        "active" => true,
        "score" => 9.5,
        "tags" => ["a", "b"],
        "meta" => %{"k" => "v"},
        "label" => :anything
      }

      assert {:ok, _result} = StrictSchema.validate(payload)
    end

    test "integer is valid for float field" do
      payload = %{"payment_id" => "p1", "amount" => 100, "currency" => "USD"}
      assert {:ok, _result} = PaymentCreatedV1.validate(payload)
    end
  end

  # ---------------------------------------------------------------------------
  # Validation — failing
  # ---------------------------------------------------------------------------

  describe "validate/1 — invalid payloads" do
    test "returns error for missing required field" do
      # missing payment_id
      payload = %{"amount" => 9.99, "currency" => "USD"}
      assert {:error, errors} = PaymentCreatedV1.validate(payload)
      assert Enum.any?(errors, fn {field, _msg} -> field == :payment_id end)
    end

    test "returns error for wrong type" do
      payload = %{"payment_id" => "p1", "amount" => "not_a_float", "currency" => "USD"}
      assert {:error, errors} = PaymentCreatedV1.validate(payload)
      assert Enum.any?(errors, fn {field, _msg} -> field == :amount end)
    end

    test "returns multiple errors for multiple violations" do
      payload = %{}
      assert {:error, errors} = PaymentCreatedV1.validate(payload)
      # payment_id and amount are required
      assert Enum.count(errors) >= 2
    end

    test "rejects non-map payload" do
      assert {:error, _errors} = PaymentCreatedV1.validate("not a map")
      assert {:error, _errors} = PaymentCreatedV1.validate(nil)
      assert {:error, _errors} = PaymentCreatedV1.validate(42)
    end
  end

  # ---------------------------------------------------------------------------
  # Migration
  # ---------------------------------------------------------------------------

  describe "migrate/2" do
    test "migrates v1 payload to v2 shape" do
      v1_payload = %{"payment_id" => "pay_001", "amount" => 9.99, "currency" => "USD"}
      migrated = PaymentCreatedV2.migrate(1, v1_payload)

      assert Map.has_key?(migrated, "amount_cents")
      assert migrated["amount_cents"] == 999
      refute Map.has_key?(migrated, "amount")
    end

    test "default migrate/2 returns payload unchanged" do
      payload = %{"payment_id" => "p1", "amount" => 5.0}
      assert PaymentCreatedV1.migrate(1, payload) == payload
    end
  end

  # ---------------------------------------------------------------------------
  # Schema.decode/3
  # ---------------------------------------------------------------------------

  describe "Schema.decode/3" do
    test "decodes valid current-version payload" do
      payload = %{"payment_id" => "p1", "amount_cents" => 500, "currency" => "USD"}
      assert {:ok, decoded} = Schema.decode(PaymentCreatedV2, payload)
      assert decoded["payment_id"] == "p1"
    end

    test "migrates v1 payload when header says version 1" do
      v1_payload = %{"payment_id" => "p1", "amount" => 5.0, "currency" => "USD"}
      headers = %{"x-schema-version" => "1"}

      assert {:ok, decoded} = Schema.decode(PaymentCreatedV2, v1_payload, headers)
      assert decoded["amount_cents"] == 500
    end

    test "returns error for incompatible version" do
      payload = %{"payment_id" => "p1", "amount" => 5.0}
      # not in compatible_versions
      headers = %{"x-schema-version" => "99"}

      assert {:error, {:incompatible_version, 99, 2}} =
               Schema.decode(PaymentCreatedV2, payload, headers)
    end

    test "defaults to current version when no header" do
      payload = %{"payment_id" => "p1", "amount_cents" => 1000, "currency" => "USD"}
      assert {:ok, _result} = Schema.decode(PaymentCreatedV2, payload)
    end
  end

  # ---------------------------------------------------------------------------
  # Schema.Registry
  # ---------------------------------------------------------------------------

  describe "Schema.Registry" do
    setup do
      start_supervised!(Registry)
      :ok
    end

    test "register/1 stores the schema module" do
      Registry.register(PaymentCreatedV1)
      assert {:ok, ^PaymentCreatedV1} = Registry.lookup("test.payments.created")
    end

    test "lookup/1 returns :not_found for unknown topic" do
      assert {:error, :not_found} = Registry.lookup("unknown.topic.xyz")
    end

    test "all/0 returns all registered modules" do
      Registry.register(PaymentCreatedV1)
      Registry.register(PaymentCreatedV2)
      all = Registry.all()
      assert PaymentCreatedV1 in all
      assert PaymentCreatedV2 in all
    end

    test "versions/1 returns all versions for a topic sorted ascending" do
      # Register the same topic under different versions
      Registry.register(PaymentCreatedV1)
      versions = Registry.versions("test.payments.created")
      assert is_list(versions)
      assert Enum.all?(versions, fn {ver, mod} -> is_integer(ver) and is_atom(mod) end)
    end

    test "lookup returns latest version when multiple registered" do
      # Both V1 and V2 use different topics in our test schemas,
      # so register manually with the same topic
      defmodule OldEvent do
        use PhoenixMicro.Schema
        schema_version(1)
        topic("shared.event")
        field(:data, :string, required: true)
      end

      defmodule NewEvent do
        use PhoenixMicro.Schema
        schema_version(2)
        topic("shared.event")
        field(:data, :string, required: true)
        field(:extra, :string, required: false)
      end

      Registry.register(OldEvent)
      Registry.register(NewEvent)

      {:ok, latest} = Registry.lookup("shared.event")
      assert latest.schema_version() == 2
    end
  end

  # ---------------------------------------------------------------------------
  # do_validate/2 (internal, tested directly)
  # ---------------------------------------------------------------------------

  describe "Schema.do_validate/2" do
    test "validates each field type" do
      fields = [
        {:name, :string, [required: true]},
        {:count, :integer, [required: true]},
        {:ratio, :float, [required: false]},
        {:on, :boolean, [required: true]}
      ]

      assert {:ok, _result} =
               Schema.do_validate(fields, %{
                 "name" => "test",
                 "count" => 5,
                 "ratio" => 0.5,
                 "on" => true
               })
    end

    test "collects all errors" do
      fields = [
        {:a, :string, [required: true]},
        {:b, :integer, [required: true]}
      ]

      assert {:error, errors} = Schema.do_validate(fields, %{})
      assert Enum.count(errors) == 2
    end
  end
end
