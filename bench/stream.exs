IO.puts("This benchmark is based on https://github.com/ClickHouse/ch-bench\n")

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

statement = fn limit ->
  "SELECT number FROM system.numbers_mt LIMIT #{limit}"
end

run_stream = fn statement, opts ->
  f = fn conn -> conn |> Ch.stream(statement, [], opts) |> Stream.run() end
  Ch.run(conn, f, timeout: :infinity)
end

Benchee.run(
  %{
    "stream without decode" => fn statement ->
      run_stream.(statement, _opts = [])
    end,
    # TODO why is this faster?
    "stream with manual decode" => fn statement ->
      f = fn conn ->
        conn
        |> Ch.stream(statement, [], format: "RowBinary")
        |> Stream.map(fn responses ->
          Enum.each(responses, fn
            {:data, _ref, data} -> Ch.RowBinary.decode_rows(data, [:u64])
            {:status, _ref, 200} -> :ok
            {:headers, _ref, _headers} -> :ok
            {:done, _ref} -> :ok
          end)
        end)
        |> Stream.run()
      end

      Ch.run(conn, f, timeout: :infinity)
    end,
    "stream with decode" => fn statement ->
      run_stream.(statement, types: [:u64])
    end
  },
  inputs: %{
    "500 rows" => statement.(500),
    "500_000 rows" => statement.(500_000),
    "500_000_000 rows" => statement.(500_000_000)
  }
)
