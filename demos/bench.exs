Mix.install([{:ch, github: "plausible/ch", branch: "bench"}])

# https://github.com/ClickHouse/ch-bench#benchmarks

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

statement = "SELECT number FROM system.numbers_mt LIMIT 500000000"
IO.puts("Running #{statement}")
start_time_ms = System.monotonic_time(:millisecond)

Ch.run(
  conn,
  fn conn ->
    conn
    |> Ch.stream(statement, [], format: "RowBinary")
    |> Stream.each(fn responses ->
      Enum.each(responses, fn
        {:data, _ref, data} -> Ch.RowBinary.decode_rows(data, [:u64])
        {:status, _ref, 200} -> :ok
        {:headers, _ref, _headers} -> :ok
        {:done, _ref} -> :ok
      end)
    end)
    |> Stream.run()
  end
)

end_time_ms = System.monotonic_time(:millisecond)

IO.puts("Finished in: #{end_time_ms - start_time_ms}ms")
