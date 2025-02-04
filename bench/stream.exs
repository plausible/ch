IO.puts("""
This benchmark is based on https://github.com/ClickHouse/ch-bench

It tests how quickly a client can select N rows from the system.numbers_mt table:

    SELECT number FROM system.numbers_mt LIMIT {limit:UInt64} FORMAT RowBinary
""")

port = String.to_integer(System.get_env("CH_PORT", "8123"))
hostname = System.get_env("CH_HOSTNAME", "localhost")
scheme = System.get_env("CH_SCHEME", "http")
username = System.get_env("CH_USERNAME", "default")
password = System.get_env("CH_PASSWORD", "default")

limits = fn limits ->
  Map.new(limits, fn limit ->
    {"limit=#{limit}", limit}
  end)
end

Benchee.run(
  %{
    # "Ch.query" => fn %{pool: pool, limit: limit} ->
    #   Ch.query!(
    #     pool,
    #     "SELECT number FROM system.numbers_mt LIMIT {limit:UInt64}",
    #     %{"limit" => limit},
    #     timeout: :infinity
    #   )
    # end,
    "Ch.stream w/o decoding (i.e. pass-through)" => fn %{pool: pool, limit: limit} ->
      DBConnection.run(
        pool,
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
    "Ch.stream with manual RowBinary decoding" => fn %{pool: pool, limit: limit} ->
      DBConnection.run(
        pool,
        fn conn ->
          conn
          |> Ch.stream(
            "SELECT number FROM system.numbers_mt LIMIT {limit:UInt64} FORMAT RowBinary",
            %{"limit" => limit}
          )
          |> Stream.each(fn %Ch.Result{data: data} ->
            data |> IO.iodata_to_binary() |> Ch.RowBinary.decode_rows([:u64])
          end)
          |> Stream.run()
        end,
        timeout: :infinity
      )
    end
  },
  before_scenario: fn limit ->
    {:ok, pool} =
      Ch.start_link(
        scheme: scheme,
        hostname: hostname,
        port: port,
        username: username,
        password: password,
        pool_size: 1
      )

    %{pool: pool, limit: limit}
  end,
  inputs: limits.([500, 500_000, 500_000_000])
)
