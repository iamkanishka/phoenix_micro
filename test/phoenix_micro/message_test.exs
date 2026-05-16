defmodule PhoenixMicro.MessageTest do
  use ExUnit.Case, async: true

  alias PhoenixMicro.Message

  describe "new/3" do
    test "creates message with required fields" do
      msg = Message.new("payments.created", %{amount: 100})

      assert msg.topic == "payments.created"
      assert msg.payload == %{amount: 100}
      assert msg.attempt == 1
      assert msg.acked? == false
      assert msg.headers == %{}
      assert msg.metadata == %{}
      assert is_binary(msg.id)
      assert %DateTime{} = msg.timestamp
    end

    test "accepts custom id" do
      msg = Message.new("test.topic", "payload", id: "custom-id-123")
      assert msg.id == "custom-id-123"
    end

    test "accepts custom headers" do
      msg = Message.new("test.topic", "payload", headers: %{"x-source" => "api"})
      assert msg.headers == %{"x-source" => "api"}
    end

    test "accepts reply_to and correlation_id" do
      msg =
        Message.new("test.topic", "payload",
          reply_to: "_inbox_123",
          correlation_id: "corr-abc"
        )

      assert msg.reply_to == "_inbox_123"
      assert msg.correlation_id == "corr-abc"
    end

    test "accepts attempt override" do
      msg = Message.new("test.topic", "payload", attempt: 3)
      assert msg.attempt == 3
    end
  end

  describe "generate_id/0" do
    test "generates a valid UUID v4 string" do
      id = Message.generate_id()
      assert is_binary(id)
      assert byte_size(id) == 36
      # UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
      assert Regex.match?(
               ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
               id
             )
    end

    test "generates unique IDs" do
      ids = for _i <- 1..1_000, do: Message.generate_id()
      assert Enum.count(Enum.uniq(ids)) == 1_000
    end
  end

  describe "increment_attempt/1" do
    test "increments the attempt counter" do
      msg = Message.new("test", "payload")
      assert msg.attempt == 1

      msg2 = Message.increment_attempt(msg)
      assert msg2.attempt == 2

      msg3 = Message.increment_attempt(msg2)
      assert msg3.attempt == 3
    end

    test "does not mutate other fields" do
      msg = Message.new("test", "payload", id: "fixed-id")
      incremented = Message.increment_attempt(msg)

      assert incremented.id == "fixed-id"
      assert incremented.topic == "test"
      assert incremented.payload == "payload"
    end
  end

  describe "ack/1" do
    test "marks message as acked" do
      msg = Message.new("test", "payload")
      refute msg.acked?
      assert Message.ack(msg).acked?
    end
  end

  describe "put_metadata/2" do
    test "merges metadata" do
      msg = Message.new("test", "payload", metadata: %{partition: 0})
      updated = Message.put_metadata(msg, %{offset: 42})

      assert updated.metadata == %{partition: 0, offset: 42}
    end

    test "overwrites existing keys" do
      msg = Message.new("test", "payload", metadata: %{attempt: 1})
      updated = Message.put_metadata(msg, %{attempt: 2})

      assert updated.metadata.attempt == 2
    end
  end

  describe "put_header/3" do
    test "adds a header" do
      msg = Message.new("test", "payload")
      updated = Message.put_header(msg, "x-trace-id", "abc123")

      assert updated.headers["x-trace-id"] == "abc123"
    end

    test "overwrites existing header" do
      msg = Message.new("test", "payload", headers: %{"x-trace-id" => "old"})
      updated = Message.put_header(msg, "x-trace-id", "new")

      assert updated.headers["x-trace-id"] == "new"
    end
  end
end
