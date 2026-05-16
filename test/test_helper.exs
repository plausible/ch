# check if clickhouse is available
case Help.http("http://localhost:8123/ping") do
  {:ok, 200, _headers, "Ok.\n"} ->
    :ok

  other ->
    Mix.shell().error("""
    ClickHouse is not detected at localhost:8123. Please start the local container with the following command:

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

ExUnit.start(exclude: exclude)
