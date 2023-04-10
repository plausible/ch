Mix.install([
  {:ch, github: "plausible/ch", branch: "bench"},
  {:benchee, "~> 1.1"}
])

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"
database = System.get_env("CH_DATABASE") || "ch_bench"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)
Ch.query!(conn, "CREATE DATABASE IF NOT EXISTS{$0:Identifier}", [database])
Ch.query!(conn, "CREATE TABLE IF NOT EXISTS #{database}.test_table (id UInt64) ENGINE = Null")

Benchee.run(
  %{
    "control" => fn rows ->
      rows |> Stream.chunk_every(60_000) |> Stream.run()
    end,
    "encode" => fn rows ->
      rows
      |> Stream.chunk_every(60_000)
      |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, [:u64]) end)
      |> Stream.run()
    end,
    "insert" => fn rows ->
      stream =
        rows
        |> Stream.chunk_every(60_000)
        |> Stream.map(fn chunk -> Ch.RowBinary.encode_rows(chunk, [:u64]) end)

      Ch.query!(conn, "INSERT INTO #{database}.test_table FORMAT RowBinary", stream, timeout: :infinity)
    end
  },
  memory_time: 2,
  inputs: %{
    "small (500 rows)" => Stream.map(1..500, fn i -> [i] end),
    "medium (500_000 rows)" => Stream.map(1..500_000, fn i -> [i] end),
    "large (500_000_000 rows)" => Stream.map(1..500_000_000, fn i -> [i] end)
  }
)
