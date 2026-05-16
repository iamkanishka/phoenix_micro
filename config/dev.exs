import Config

# Development: use memory transport so no broker is required.
# Attach the default logger to see all events in iex.
config :phoenix_micro,
  transport: :memory,
  transports: [memory: []],
  telemetry_enabled: true

# Optionally switch to a real broker in dev:
#
# config :phoenix_micro,
#   transport: :rabbitmq,
#   transports: [
#     rabbitmq: [
#       url: "amqp://guest:guest@localhost",
#       exchange: "dev_phoenix_micro",
#       prefetch_count: 5
#     ]
#   ]
