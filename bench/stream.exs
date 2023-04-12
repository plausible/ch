IO.puts("This benchmark is based on https://github.com/ClickHouse/ch-bench\n")

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

statement = fn limit ->
  "SELECT number FROM system.numbers_mt LIMIT #{limit}"
end

run_stream = fn statement, f ->
  Ch.run(
    conn,
    fn conn -> conn |> Ch.stream(statement, [], format: "RowBinary") |> f.() end,
    timeout: :infinity
  )
end

Benchee.run(
  %{
    "stream without decode" => fn count ->
      statement = statement.(count)
      run_stream.(statement, fn stream -> Stream.run(stream) end)
    end,
    "stream with decode" => fn count ->
      statement = statement.(count)

      run_stream.(statement, fn stream ->
        stream
        |> Stream.each(fn responses ->
          Enum.each(responses, fn
            {:data, _ref, data} -> Ch.RowBinary.decode_rows(data, [:u64])
            {:status, _ref, 200} -> :ok
            {:headers, _ref, _headers} -> :ok
            {:done, _ref} -> :ok
          end)
        end)
        |> Stream.run()
      end)
    end
  },
  inputs: %{
    "small (500 rows)" => 500,
    "medium (500_000 rows)" => 500_000,
    "large (500_000_000 rows)" => 500_000_000
  }
)
