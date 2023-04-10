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
Ch.query!(conn, "CREATE TABLE IF NOT EXISTS test_table (id UInt64) ENGINE = Null")

rows_500 = Enum.map(1..500, fn i -> [i] end)
rows_50000 = Enum.map(1..50000, fn i -> [i] end)

Benchee.run(
  %{
    "ch" => fn chunks ->
      stream = Stream.map(chunks, fn chunk -> Ch.RowBinary.encode_rows(chunk, [:u64]) end)
      Ch.query!(conn, "INSERT INTO test_table FORMAT RowBinary", stream, timeout: :infinity)
    end
  },
  memory_time: 2,
  inputs: %{
    "small (500 rows)" => [rows_500],
    "medium (500_000 rows)" => Stream.repeatedly(fn -> rows_50000 end) |> Stream.take(10),
    "large (500_000_000 rows)" => Stream.repeatedly(fn -> rows_50000 end) |> Stream.take(10000)
  }
)
