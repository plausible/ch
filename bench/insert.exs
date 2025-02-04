IO.puts("""
This benchmark is based on https://github.com/ClickHouse/clickhouse-go#benchmark

It tests how quickly a client can insert one million rows of the following schema:
- col1 UInt64
- col2 String
- col3 Array(UInt8)
- col4 DateTime
""")

port = String.to_integer(System.get_env("CH_PORT", "8123"))
hostname = System.get_env("CH_HOSTNAME", "localhost")
scheme = System.get_env("CH_SCHEME", "http")
database = System.get_env("CH_DATABASE", "ch_bench")
username = System.get_env("CH_USERNAME", "default")
password = System.get_env("CH_PASSWORD", "default")

alias Ch.RowBinary

rows = fn count ->
  Enum.map(1..count, fn i ->
    [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], DateTime.utc_now()]
  end)
end

statement = "INSERT INTO #{database}.benchmark FORMAT RowBinary"
types = ["UInt64", "String", "Array(UInt8)", "DateTime"]

Benchee.run(
  %{
    "Ch.query" => fn %{pool: pool, rows: rows} ->
      Ch.query!(pool, statement, rows, types: types)
    end,
    "Ch.stream" => fn %{pool: pool, rows: rows} ->
      DBConnection.run(pool, fn conn ->
        Stream.chunk_every(rows, 100_000)
        |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, types) end)
        |> Stream.into(Ch.stream(conn, statement, [], encode: false))
        |> Stream.run()
      end)
    end
  },
  before_scenario: fn rows ->
    {:ok, pool} =
      Ch.start_link(
        scheme: scheme,
        hostname: hostname,
        port: port,
        username: username,
        password: password,
        pool_size: 1
      )

    Ch.query!(pool, "CREATE DATABASE IF NOT EXISTS {$0:Identifier}", [database])

    Ch.query!(pool, """
    CREATE TABLE IF NOT EXISTS #{database}.benchmark (
      col1 UInt64,
      col2 String,
      col3 Array(UInt8),
      col4 DateTime
    ) Engine Null
    """)

    %{pool: pool, rows: rows}
  end,
  inputs: %{
    "1_000_000 rows" => rows.(1_000_000)
  }
)
