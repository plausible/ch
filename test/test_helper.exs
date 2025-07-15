clickhouse_available? =
  case :httpc.request(:get, {~c"http://localhost:8123/ping", []}, [], []) do
    {:ok, {{_version, _status = 200, _reason}, _headers, ~c"Ok.\n"}} ->
      true

    {:error, {:failed_connect, [{:to_address, _to_address}, {:inet, [:inet], :econnrefused}]}} ->
      false
  end

unless clickhouse_available? do
  Mix.shell().error("""
  ClickHouse is not detected at localhost:8123! Please start the local container with the following command:

      docker compose up -d clickhouse
  """)

  System.halt(1)
end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)
default_test_db = System.get_env("CH_DATABASE", "ch_elixir_test")
Application.put_env(:ch, :database, default_test_db)

Ch.Test.query("DROP DATABASE IF EXISTS {db:Identifier}", %{"db" => default_test_db})
Ch.Test.query("CREATE DATABASE {db:Identifier}", %{"db" => default_test_db})

%{rows: [[ch_version]]} = Ch.Test.query("SELECT version()")

extra_exclude =
  if ch_version >= "25" do
    []
  else
    # Time type is not supported in ClickHouse < 25
    [:time]
  end

ExUnit.start(exclude: [:slow | extra_exclude])
