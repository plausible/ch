IO.puts("This benchmark is based on https://github.com/ClickHouse/ch-bench\n")

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

statement = fn limit ->
  "SELECT number FROM system.numbers_mt LIMIT #{limit}"
end

Benchee.run(
  %{
    "RowBinary stream without decode" => fn statement ->
      DBConnection.run(
        conn,
        fn conn ->
          conn
          |> Ch.stream(statement, _params = [], format: "RowBinary")
          |> Stream.run()
        end,
        timeout: :infinity
      )
    end,
    "RowBinary stream with manual decode" => fn statement ->
      DBConnection.run(conn, fn conn ->
        conn
        |> Ch.stream(statement, _params = [], format: "RowBinary")
        |> Stream.map(fn %Ch.Result{data: data} ->
          data
          |> IO.iodata_to_binary()
          |> Ch.RowBinary.decode_rows([:u64])
        end)
        |> Stream.run()
      end)
    end
  },
  inputs: %{
    "500 rows" => statement.(500),
    "500_000 rows" => statement.(500_000),
    "500_000_000 rows" => statement.(500_000_000)
  }
)
