[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: [
    # Consumer DSL macros
    topic: 1,
    concurrency: 1,
    retry: 1,
    middleware: 1,
    dead_letter_topic: 1,
    transport: 1,
    queue_group: 1
  ]
]
