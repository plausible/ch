IO.puts("This benchmark is based on https://github.com/ClickHouse/clickhouse-go#benchmark\n")

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

types = [Ch.Types.u64(), Ch.Types.string(), Ch.Types.array(Ch.Types.u8()), Ch.Types.datetime()]
statement = "INSERT INTO #{database}.benchmark FORMAT RowBinary"

rows = fn count ->
  Enum.map(1..count, fn i ->
    [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], NaiveDateTime.utc_now()]
  end)
end

alias Ch.RowBinary

Benchee.run(
  %{
    # "control" => fn rows -> Enum.each(rows, fn _row -> :ok end) end,
    "encode" => fn rows -> RowBinary.encode_rows(rows, types) end,
    "insert" => fn rows -> Ch.query!(conn, statement, rows, types: types) end,
    # "control stream" => fn rows -> rows |> Stream.chunk_every(60_000) |> Stream.run() end,
    "encode stream" => fn rows ->
      rows
      |> Stream.chunk_every(60_000)
      |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, types) end)
      |> Stream.run()
    end,
    "insert stream" => fn rows ->
      stream =
        rows
        |> Stream.chunk_every(60_000)
        |> Stream.map(fn chunk -> RowBinary.encode_rows(chunk, types) end)

      Ch.query!(conn, statement, stream, encode: false)
    end
  },
  inputs: %{
    "1_000_000 rows" => rows.(1_000_000)
  }
)
