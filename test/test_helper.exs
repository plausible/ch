# check if clickhouse is available
case Help.http("http://localhost:8123/ping") do
  {:ok, 200, _headers, "Ok.\n"} ->
    :ok

  other ->
    Mix.shell().error("""
    ClickHouse is not detected at localhost:8123:

    #{inspect(other)}

    Please start the container with the following command:

        docker compose up -d clickhouse
    """)

    System.halt(1)
end

%{rows: [[ch_version]]} = Help.ch("SELECT version()")

exclude =
  if ch_version >= "25" do
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
