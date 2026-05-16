url = "http://localhost:8123"

{:ok, _pid} = Ch.start_link(name: Ch.TestPool, url: url, pool_size: 100)

version =
  case Ch.query(Ch.TestPool, "select version()") do
    {:ok, %{names: ["version"], rows: [[version]]}} ->
      version

    {:error, reason} ->
      Mix.shell().error("""
      ClickHouse is not detected at #{url}: #{Exception.message(reason)}

      Please start the container with the following command:

          docker compose up -d clickhouse
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

if System.get_env("CI") do
  Application.put_env(:stream_data, :max_runs, 1000)
end

ExUnit.start(exclude: exclude, assert_receive_timeout: assert_receive_timeout)
