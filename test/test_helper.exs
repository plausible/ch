# TODO
# clickhouse_available? =
#   case Help.http("http://localhost:8123/ping") do
#     {200, _headers, "Ok.\n"} -> true
#     {:error, :econnrefused} -> false
#   end

# unless clickhouse_available? do
#   Mix.shell().error("""
#   ClickHouse is not detected at localhost:8123! Please start the local container with the following command:

#       docker compose up -d clickhouse
#   """)

#   System.halt(1)
# end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)
ExUnit.start()
