IO.puts("This benchmark is based on https://github.com/ClickHouse/ch-bench\n")

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

Benchee.run(
  %{
    "RowBinary stream without decode" => fn limit ->
      DBConnection.run(
        conn,
        fn conn ->
          conn
          |> Ch.stream(
            "SELECT number FROM system.numbers_mt LIMIT {limit:UInt64} FORMAT RowBinary",
            %{"limit" => limit}
          )
          |> Stream.run()
        end,
        timeout: :infinity
      )
    end,
    "RowBinary stream with manual decode" => fn limit ->
      DBConnection.run(
        conn,
        fn conn ->
          conn
          |> Ch.stream(
            "SELECT number FROM system.numbers_mt LIMIT {limit:UInt64} FORMAT RowBinary",
            %{"limit" => limit}
          )
          |> Stream.map(fn %Ch.Result{data: data} ->
            data
            |> IO.iodata_to_binary()
            |> Ch.RowBinary.decode_rows([:u64])
          end)
          |> Stream.run()
        end,
        timeout: :infinity
      )
    end
  },
  inputs: %{
    "500 rows" => 500,
    "500_000 rows" => 500_000,
    "500_000_000 rows" => 500_000_000
  }
)
