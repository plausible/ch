Mix.install([
  {:ch, github: "plausible/ch", branch: "bench"},
  {:benchee, "~> 1.1"}
])

port = String.to_integer(System.get_env("CH_PORT") || "8123")
hostname = System.get_env("CH_HOSTNAME") || "localhost"
scheme = System.get_env("CH_SCHEME") || "http"

{:ok, conn} = Ch.start_link(scheme: scheme, hostname: hostname, port: port)

Benchee.run(
  %{
    "ch" => fn statement ->
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
        end,
        timeout: :infinity
      )
    end
  },
  memory_time: 2,
  inputs: %{
    "small (500 rows)" => "SELECT number FROM system.numbers_mt LIMIT 500",
    "medium (500_000 rows)" => "SELECT number FROM system.numbers_mt LIMIT 500000",
    "large (500_000_000 rows)" => "SELECT number FROM system.numbers_mt LIMIT 500000000"
  }
)
