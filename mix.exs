defmodule PhoenixMicro.MixProject do
  use Mix.Project

  @version "1.0.0"
  @source_url "https://github.com/iamkanishka/phoenix_micro"
  @description "Production-grade microservices toolkit for Elixir/Phoenix — transports, sagas, RPC, schema registry, circuit breakers, outbox pattern, and more."

  def project do
    [
      app: :phoenix_micro,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),
      aliases: aliases(),

      # Hex
      description: @description,
      package: package(),

      # Docs
      name: "PhoenixMicro",
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs(),

      # Test
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :inets, :ssl, :mix],
      mod: {PhoenixMicro.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Required
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.1"},
      {:telemetry, "~> 1.2"},

      # Broadway pipeline (strongly recommended for production)
      {:broadway, "~> 1.0"},

      # Transport adapters — NONE are listed as phoenix_micro deps.
      # All four transports use runtime detection (Code.ensure_loaded?) so
      # phoenix_micro itself compiles with zero native-code or rebar3 deps.
      #
      # Add the client for your chosen transport to YOUR application's mix.exs:
      #   RabbitMQ  → {:amqp, "~> 3.3"}
      #              (pulls rabbit_common / rebar3 — needs Erlang rebar3 toolchain)
      #   NATS      → {:gnat, "~> 1.7"}           (pure Elixir, no rebar3)
      #   Redis     → {:redix, "~> 1.5"}           (pure Elixir, no rebar3)
      #   Kafka     → no dep needed! Pure Elixir Kafka client is built-in.

      # Outbox pattern (optional — requires Ecto)
      {:ecto_sql, "~> 3.11", optional: true},
      {:postgrex, "~> 0.18", optional: true},

      # Observability (optional)
      {:telemetry_metrics, "~> 1.0", optional: true},
      {:opentelemetry, "~> 1.3", optional: true},
      {:opentelemetry_api, "~> 1.3", optional: true},

      # LiveDashboard integration (optional)
      {:phoenix_live_dashboard, "~> 0.8", optional: true},

      # Dev / test
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:stream_data, "~> 1.0", only: :test},
      {:mox, "~> 1.1", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:redix, "~> 1.5"}
    ]
  end

  defp package do
    [
      maintainers: ["Kanishka Naik"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "HexDocs" => "https://hexdocs.pm/phoenix_micro"
      },
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md guides)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "README.md": [title: "Overview"],
        "CHANGELOG.md": [title: "Changelog"],
        "guides/getting_started.md": [title: "Getting Started"],
        "guides/transports.md": [title: "Transports"],
        "guides/sagas.md": [title: "Sagas & Compensation"],
        "guides/schemas.md": [title: "Schema Registry"],
        "guides/middleware.md": [title: "Middleware"],
        "guides/rpc.md": [title: "RPC"],
        "guides/outbox.md": [title: "Outbox Pattern"],
        "guides/testing.md": [title: "Testing"],
        "guides/deployment.md": [title: "Deployment & Production"]
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ],
      groups_for_modules: [
        Core: [
          PhoenixMicro,
          PhoenixMicro.Consumer,
          PhoenixMicro.Message,
          PhoenixMicro.Producer,
          PhoenixMicro.RPC,
          PhoenixMicro.Config
        ],
        Transports: [
          PhoenixMicro.Transport,
          PhoenixMicro.Transport.Memory,
          PhoenixMicro.Transport.RabbitMQ,
          PhoenixMicro.Transport.Kafka,
          PhoenixMicro.Transport.NATS,
          PhoenixMicro.Transport.RedisStreams
        ],
        "Schema & Validation": [
          PhoenixMicro.Schema,
          PhoenixMicro.Schema.Registry,
          PhoenixMicro.Schema.Validator,
          PhoenixMicro.Schema.Migrator,
          PhoenixMicro.Schema.Field,
          PhoenixMicro.Serializer
        ],
        Middleware: [
          PhoenixMicro.Middleware,
          PhoenixMicro.Middleware.Logger,
          PhoenixMicro.Middleware.Metrics,
          PhoenixMicro.Middleware.Retry,
          PhoenixMicro.Middleware.Tracing,
          PhoenixMicro.Middleware.Idempotency,
          PhoenixMicro.Middleware.CircuitBreaker,
          PhoenixMicro.IdempotencyStore
        ],
        Sagas: [PhoenixMicro.Saga],
        Outbox: [PhoenixMicro.Outbox],
        Observability: [
          PhoenixMicro.Telemetry,
          PhoenixMicro.Phoenix.HealthPlug,
          PhoenixMicro.Phoenix.MetricsStore,
          PhoenixMicro.LiveDashboard.Page
        ],
        Utilities: [
          PhoenixMicro.Utils.Backoff,
          PhoenixMicro.Utils.Encoding,
          PhoenixMicro.Utils.ID,
          PhoenixMicro.Utils.WorkerPool
        ]
      ],
      before_closing_head_tag: &before_closing_head_tag/1,
      before_closing_body_tag: &before_closing_body_tag/1
    ]
  end

  defp before_closing_head_tag(:html) do
    """
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&display=swap">
    """
  end

  defp before_closing_head_tag(_), do: ""

  defp before_closing_body_tag(:html), do: ""
  defp before_closing_body_tag(_), do: ""

  defp aliases do
    [
      setup: ["deps.get"],
      "test.all": ["test --include integration"],
      quality: ["format --check-formatted", "credo --strict", "dialyzer"],
      "quality.fix": ["format", "credo --strict"]
    ]
  end
end
