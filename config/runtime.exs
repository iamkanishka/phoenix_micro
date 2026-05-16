import Config

# Runtime config — evaluated after the release is assembled.
# Environment variables take precedence over compile-time config.

if config_env() == :prod do
  transport = System.get_env("PHOENIX_MICRO_TRANSPORT", "rabbitmq") |> String.to_atom()

  config :phoenix_micro,
    transport: transport,
    # Register consumers in your Application.start/2
    consumers: []

  case transport do
    :rabbitmq ->
      config :phoenix_micro,
        transports: [
          rabbitmq: [
            url: System.fetch_env!("RABBITMQ_URL"),
            exchange: System.get_env("RABBITMQ_EXCHANGE", "phoenix_micro"),
            prefetch_count: System.get_env("RABBITMQ_PREFETCH", "10") |> String.to_integer(),
            reconnect_interval: 2_000
          ]
        ]

    :kafka ->
      brokers =
        System.fetch_env!("KAFKA_BROKERS")
        |> String.split(",")
        |> Enum.map(fn broker ->
          [host, port] = String.split(String.trim(broker), ":")
          {host, String.to_integer(port)}
        end)

      config :phoenix_micro,
        transports: [
          kafka: [
            brokers: brokers,
            group_id: System.fetch_env!("KAFKA_GROUP_ID"),
            client_id: String.to_atom(System.get_env("KAFKA_CLIENT_ID", "phoenix_micro")),
            begin_offset: :latest
          ]
        ]

    :nats ->
      config :phoenix_micro,
        transports: [
          nats: [
            host: System.fetch_env!("NATS_HOST"),
            port: System.get_env("NATS_PORT", "4222") |> String.to_integer(),
            username: System.get_env("NATS_USERNAME"),
            password: System.get_env("NATS_PASSWORD")
          ]
        ]

    :redis_streams ->
      config :phoenix_micro,
        transports: [
          redis_streams: [
            url: System.fetch_env!("REDIS_URL"),
            consumer_group: System.fetch_env!("REDIS_CONSUMER_GROUP"),
            consumer_name: System.get_env("REDIS_CONSUMER_NAME", "#{node()}")
          ]
        ]

    :memory ->
      config :phoenix_micro, transports: [memory: []]
  end

  config :phoenix_micro,
    default_timeout: System.get_env("PHOENIX_MICRO_TIMEOUT", "5000") |> String.to_integer(),
    default_retry: [
      max_attempts: System.get_env("PHOENIX_MICRO_MAX_RETRIES", "5") |> String.to_integer(),
      base_delay: 500,
      max_delay: 30_000,
      jitter: true
    ]
end
