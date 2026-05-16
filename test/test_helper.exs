ExUnit.start(exclude: [:integration])

Application.put_env(:phoenix_micro, :transport, :memory)
Application.put_env(:phoenix_micro, :transports, memory: [])
Application.put_env(:phoenix_micro, :default_timeout, 2_000)

Application.put_env(:phoenix_micro, :default_retry,
  max_attempts: 2,
  base_delay: 50,
  max_delay: 200
)
