IO.puts("This benchmarks is based on https://github.com/ClickHouse/clickhouse-go#benchmark\n")

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"
database = System.get_env("CH_DATABASE") || "ch_bench"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)
Ch.query!(conn, "CREATE DATABASE IF NOT EXISTS {$0:Identifier}", [database])

Ch.query!(conn, """
CREATE TABLE IF NOT EXISTS #{database}.benchmark (
  col1 UInt64,
  col2 String,
  col3 Array(UInt8),
  col4 DateTime
) Engine Null
""")

types = [Ch.u64(), Ch.string(), Ch.array(Ch.u8()), Ch.datetime()]
statement = "INSERT INTO #{database}.benchmark FORMAT RowBinary"

Benchee.run(
  %{
    "control" => fn rows ->
      rows |> Stream.chunk_every(60_000) |> Stream.run()
    end,
    "encode" => fn rows ->
      rows
      |> Stream.chunk_every(60_000)
      |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, types) end)
      |> Stream.run()
    end,
    "insert" => fn rows ->
      stream =
        rows
        |> Stream.chunk_every(60_000)
        |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, types) end)

      Ch.query!(conn, statement, {:raw, stream})
    end
  },
  inputs: %{
    "medium (1_000_000 rows)" =>
      Stream.map(1..1_000_000, fn i ->
        [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], NaiveDateTime.utc_now()]
      end)
  }
)
