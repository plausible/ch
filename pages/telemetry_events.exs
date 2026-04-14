system_time = [
  system_time: [type: "`integer()`", doc: "System time in native time units."]
]

duration = [
  duration: [type: "`integer()`", doc: "Duration in native time units."]
]

exception_meta = [
  kind: [type: "`atom()`", doc: "One of `:throw`, `:error`, or `:exit`."],
  reason: [type: "`term()`", doc: "The exception reason."],
  stacktrace: [type: "`Exception.stacktrace()`", doc: "The exception stacktrace."]
]

query_meta = [
  pool: [type: "`NimblePool.pool()`", doc: "The pool name or pid."],
  statement: [type: "`iodata()`", doc: "The query statement."]
]

pool_meta = [
  scheme: [type: "`atom()`", doc: "The connection scheme (e.g. `:http`)."],
  host: [type: "`String.t()`", doc: "The host name."],
  port: [type: "`:inet.port_number()`", doc: "The port number."]
]

[
  [
    title: "Query Events",
    doc: "Events emitted during query execution.",
    events: [
      "[:ch, :query, :start]": [
        doc: "Emitted when a query execution starts.",
        measurements: system_time,
        metadata: query_meta
      ],
      "[:ch, :query, :stop]": [
        doc: "Emitted when a query completes successfully.",
        measurements: [
          encode_time: [
            type: "`integer()`",
            doc: "Time spent encoding the request in native units."
          ],
          queue_time: [
            type: "`integer()`",
            doc: "Time spent waiting for a connection in native units."
          ],
          query_time: [
            type: "`integer()`",
            doc: "Time spent executing the request over the network in native units."
          ],
          decode_time: [
            type: "`integer()`",
            doc: "Time spent decoding the response in native units."
          ],
          total_time: [
            type: "`integer()`",
            doc: "Total time from start to stop in native units."
          ],
          idle_time: [
            type: "`integer()`",
            doc: "Time the connection spent idle in the pool prior to this query."
          ]
        ],
        metadata:
          query_meta ++
            [result: [type: "`term()`", doc: "The query result."]]
      ],
      "[:ch, :query, :exception]": [
        doc: "Emitted when a query raises an exception.",
        measurements: duration,
        metadata: query_meta ++ exception_meta
      ]
    ]
  ],
  [
    title: "Pool Events",
    doc: "Events emitted by the connection pool.",
    events: [
      "[:ch, :pool, :connect, :start]": [
        doc: "Emitted when a TCP/TLS connection attempt starts.",
        measurements: system_time,
        metadata: pool_meta
      ],
      "[:ch, :pool, :connect, :stop]": [
        doc: "Emitted when a TCP/TLS connection attempt completes.",
        measurements: duration,
        metadata:
          pool_meta ++
            [
              result: [
                type: "`{:ok, Mint.HTTP1.t()} | {:error, term()}`",
                doc: "The result of the connection attempt."
              ]
            ]
      ],
      "[:ch, :pool, :connect, :exception]": [
        doc: "Emitted when a TCP/TLS connection attempt raises an exception.",
        measurements: duration,
        metadata: pool_meta ++ exception_meta
      ],
      "[:ch, :pool, :reused_connection]": [
        doc: "Emitted when an existing connection is successfully checked out of the pool.",
        measurements: system_time,
        metadata: pool_meta
      ],
      "[:ch, :pool, :disconnect]": [
        doc: "Emitted when a connection is closed and removed from the pool.",
        measurements: system_time,
        metadata:
          pool_meta ++
            [
              reason: [type: "`term()`", doc: "The reason for disconnection."]
            ]
      ],
      "[:ch, :pool, :connection_idle]": [
        doc:
          "Emitted when a connection is checked out, tracking how long it sat idle in the pool.",
        measurements: [
          idle_time: [
            type: "`integer()`",
            doc: "Time the connection spent idle in the pool in native units."
          ]
        ],
        metadata: pool_meta
      ]
    ]
  ]
]
