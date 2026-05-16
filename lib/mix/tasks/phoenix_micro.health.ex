defmodule Mix.Tasks.PhoenixMicro.Health do
  use Mix.Task

  @shortdoc "Checks PhoenixMicro health endpoint and prints a status report"

  @moduledoc """
  Hits the PhoenixMicro health endpoint and prints a formatted status table.

  ## Usage

      mix phoenix_micro.health

  ## Options

      --url URL       Health endpoint URL
                      (default: http://localhost:4000/health/microservices)
      --timeout MS    HTTP request timeout in ms (default: 5000)
      --format FORMAT Output format: table|json (default: table)
      --exit-code     Exit with code 1 if status is degraded (useful in CI)

  ## Examples

      # Check local dev server
      mix phoenix_micro.health

      # Check staging
      mix phoenix_micro.health --url https://staging.example.com/health/microservices

      # Use in CI — fails the pipeline if degraded
      mix phoenix_micro.health --exit-code

      # Raw JSON output
      mix phoenix_micro.health --format json

  ## Output

      ┌─────────────────────────────────────────────────┐
      │  PhoenixMicro Health Report                     │
      │  http://localhost:4000/health/microservices     │
      │  2025-01-01T12:00:00Z                           │
      └─────────────────────────────────────────────────┘

      Status      ✓ ok
      Transport   rabbitmq (connected)

      Consumers (2)
        MyApp.Payments.CreatedConsumer  payments.created   broadway  concurrency=10
        MyApp.Orders.PlacedConsumer     orders.placed      broadway  concurrency=5

      Circuit Breakers (3)
        payments_db   ✓ closed
        orders_db     ✗ OPEN    (opened 45s ago)
        inventory_db  ~ half_open

      Pipeline
        Running consumers: 2
  """

  @switches [
    url: :string,
    timeout: :integer,
    format: :string,
    exit_code: :boolean
  ]

  @default_url "http://localhost:4000/health/microservices"
  @default_timeout 5_000

  @spec run([String.t()]) :: any()
  @impl Mix.Task
  def run(argv) do
    {opts, _args, _ignored} = OptionParser.parse(argv, switches: @switches)

    url = Keyword.get(opts, :url, @default_url)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    format = Keyword.get(opts, :format, "table")
    use_exit_code = Keyword.get(opts, :exit_code, false)

    IO.puts("Checking #{url}...")

    case fetch_health(url, timeout) do
      {:ok, body} ->
        case Jason.decode(body) do
          {:ok, data} ->
            case format do
              "json" -> IO.puts(Jason.encode!(data, pretty: true))
              _fmt -> print_table(data, url)
            end

            if use_exit_code and data["status"] != "ok" do
              exit({:shutdown, 1})
            end

          {:error, _reason} ->
            IO.puts(:stderr, "Could not parse response as JSON:\n#{body}")
            if use_exit_code, do: exit({:shutdown, 1})
        end

      {:error, reason} ->
        IO.puts(:stderr, "Failed to reach health endpoint: #{inspect(reason)}")
        if use_exit_code, do: exit({:shutdown, 1})
    end
  end

  # ---------------------------------------------------------------------------
  # HTTP fetch
  # ---------------------------------------------------------------------------

  defp fetch_health(url, timeout) do
    # :httpc lives in :inets — must be started first. Capture returns to avoid
    # unmatched-return warnings (:already_started is a normal ok case).
    _inets = Application.ensure_started(:inets)
    _ssl = Application.ensure_started(:ssl)

    url_charlist = String.to_charlist(url)
    http_opts = [{:timeout, timeout}, {:connect_timeout, timeout}]
    opts = [{:body_format, :binary}]

    # Use apply/3 so :httpc is resolved at runtime (avoids undefined-function
    # warning when :inets is not in the compile-time dependency list).
    case apply(:httpc, :request, [:get, {url_charlist, []}, http_opts, opts]) do
      {:ok, {{_version, status, _reason}, _headers, body}} ->
        if status in 200..503 do
          {:ok, body}
        else
          {:error, {:unexpected_status, status}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Table formatter
  # ---------------------------------------------------------------------------

  defp print_table(data, url) do
    timestamp = Map.get(data, "timestamp", "—")
    status = Map.get(data, "status", "unknown")
    transport = Map.get(data, "transport", %{})
    consumers = Map.get(data, "consumers", [])
    cbs = Map.get(data, "circuit_breakers", [])
    pipeline = Map.get(data, "pipeline", %{})

    # Header box
    width = 54
    line = String.duplicate("─", width)
    puts("┌#{line}┐")
    puts("│  #{pad("PhoenixMicro Health Report", width - 2)}│")
    puts("│  #{pad(url, width - 2)}│")
    puts("│  #{pad(timestamp, width - 2)}│")
    puts("└#{line}┘")
    puts("")

    # Status
    status_icon = if status == "ok", do: "✓", else: "✗"
    status_colour = if status == "ok", do: :green, else: :red
    puts("  Status      #{colour(status_icon <> " " <> status, status_colour)}")

    # Transport
    t_name = Map.get(transport, "active", "unknown")
    t_status = Map.get(transport, "status", "unknown")
    t_colour = if t_status == "connected", do: :green, else: :red
    puts("  Transport   #{t_name} (#{colour(t_status, t_colour)})")
    puts("")

    # Consumers
    puts("  Consumers (#{length(consumers)})")

    if consumers == [] do
      puts("    #{colour("none registered", :yellow)}")
    else
      Enum.each(consumers, fn c ->
        mod = Map.get(c, "module", "?")
        topic = Map.get(c, "topic", "?")
        pl = Map.get(c, "pipeline", "broadway")
        concurrency = Map.get(c, "concurrency", 1)
        puts("    #{pad(mod, 40)}  #{pad(topic, 30)}  #{pl}  concurrency=#{concurrency}")
      end)
    end

    puts("")

    # Circuit Breakers
    puts("  Circuit Breakers (#{length(cbs)})")

    if cbs == [] do
      puts("    #{colour("none registered", :yellow)}")
    else
      Enum.each(cbs, fn cb ->
        fuse = Map.get(cb, "fuse", "?")
        state = Map.get(cb, "state", "?")

        {icon, col, extra} =
          case state do
            "closed" -> {"✓", :green, ""}
            "open" -> {"✗", :red, open_age(Map.get(cb, "opened_at_ms"))}
            "half_open" -> {"~", :yellow, "(probing)"}
            _state -> {"?", :default, ""}
          end

        puts("    #{pad(fuse, 30)}  #{colour(icon <> " " <> state, col)}  #{extra}")
      end)
    end

    puts("")

    # Pipeline
    running = get_in(pipeline, ["running"]) || 0
    puts("  Pipeline")
    puts("    Running consumers: #{running}")
    puts("")
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp puts(str), do: IO.puts(str)

  defp pad(str, len) when is_binary(str) do
    str_len = String.length(str)

    if str_len >= len do
      String.slice(str, 0, len)
    else
      str <> String.duplicate(" ", len - str_len)
    end
  end

  defp pad(other, len), do: pad(to_string(other), len)

  defp colour(str, :green), do: IO.ANSI.green() <> str <> IO.ANSI.reset()
  defp colour(str, :red), do: IO.ANSI.red() <> str <> IO.ANSI.reset()
  defp colour(str, :yellow), do: IO.ANSI.yellow() <> str <> IO.ANSI.reset()
  defp colour(str, :default), do: str

  defp open_age(nil), do: ""

  defp open_age(ms) when is_integer(ms) do
    now = System.system_time(:millisecond)
    # ms is a monotonic timestamp from the remote node — use as-is for delta
    ago = abs(now - ms)

    cond do
      ago < 60_000 -> "(opened #{div(ago, 1_000)}s ago)"
      ago < 3_600_000 -> "(opened #{div(ago, 60_000)}m ago)"
      true -> "(opened #{div(ago, 3_600_000)}h ago)"
    end
  end
end
