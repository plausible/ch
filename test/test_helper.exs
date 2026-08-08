url = "http://localhost:8123"

{:ok, _pid} = Help.start_link_pool(url)

version =
  case Help.query("select version()") do
    {:ok, %{rows: [[version]]}} ->
      version

    {:error, reason} ->
      Mix.shell().error("""
      ClickHouse is not detected at #{url}: #{Exception.message(reason)}

      Please start the container with the following command:

          docker compose up -d clickhouse
      """)

      System.halt(1)
  end

case Help.http("GET", "http://localhost:8474/proxies") do
  {:ok, 200, _headers, _body} ->
    :ok

  {:ok, status, _headers, body} ->
    Mix.shell().error("Toxiproxy returned unexpected status #{status}: #{body}")
    System.halt(1)

  {:error, reason} ->
    Mix.shell().error("""
    Toxiproxy is not detected at http://localhost:8474: #{Exception.message(reason)}

    Please start the container with the following command:

        docker compose up -d toxiproxy
    """)

    System.halt(1)
end

exclude =
  if version >= "25" do
    []
  else
    # Time, Variant, JSON, and Dynamic types are not supported in older ClickHouse versions we have in the CI
    [:time, :variant, :json, :dynamic]
  end

assert_receive_timeout =
  if System.get_env("CI") do
    to_timeout(second: 5)
  else
    to_timeout(second: 1)
  end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

Help.setup_toxiproxy_counter()

ExUnit.start(exclude: exclude, assert_receive_timeout: assert_receive_timeout)
