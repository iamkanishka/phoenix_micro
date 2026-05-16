import Config

# Default to memory transport — safe to run without any external broker.
# Override in environment-specific files.
config :phoenix_micro,
  transport: :memory,
  transports: [
    memory: []
  ],
  default_timeout: 5_000,
  default_retry: [
    max_attempts: 3,
    base_delay: 500,
    max_delay: 30_000,
    jitter: true
  ],
  serializer: PhoenixMicro.Serializer.JSON,
  telemetry_enabled: true,
  idempotency_store: nil,
  consumers: []

import_config "#{config_env()}.exs"
