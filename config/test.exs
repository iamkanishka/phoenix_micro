import Config

config :phoenix_micro,
  transport: :memory,
  transports: [memory: []],
  default_timeout: 2_000,
  default_retry: [
    max_attempts: 2,
    base_delay: 10,
    max_delay: 100,
    jitter: false
  ],
  telemetry_enabled: false,
  consumers: []

# Keep logger quiet during tests; individual tests use capture_log as needed
config :logger, level: :warning
