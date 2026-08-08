defmodule Ch.TimezoneTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  import Ch.Test, only: [parameterize_query!: 2, parameterize_query!: 3]

  @session_timezone "Europe/Berlin"

  setup do
    {:ok,
     conn:
       start_supervised!(
         {Ch, database: Ch.Test.database(), settings: [session_timezone: @session_timezone]}
       )}
  end

  test "session timezone is reported by ClickHouse", ctx do
    result = parameterize_query!(ctx, "SELECT timezone()")

    assert result.rows == [[@session_timezone]]

    assert List.keyfind!(result.headers, "x-clickhouse-timezone", 0) ==
             {"x-clickhouse-timezone", @session_timezone}
  end

  test "naive datetime params use the session timezone", ctx do
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d)",
             [~N[2022-01-01 12:00:00]]
           ).rows == [[~N[2022-01-01 11:00:00], "2022-01-01 12:00:00"]]

    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d)",
             [~N[2022-07-01 12:00:00]]
           ).rows == [[~N[2022-07-01 10:00:00], "2022-07-01 12:00:00"]]
  end

  test "naive datetime64 params use the session timezone", ctx do
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime64(3)} AS d, toString(d)",
             [~N[2022-01-01 12:00:00.123]]
           ).rows == [[~N[2022-01-01 11:00:00.123], "2022-01-01 12:00:00.123"]]
  end

  test "UTC datetime params keep their instant and render in the session timezone", ctx do
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d), {$1:DateTime64(3)} AS d64, toString(d64)",
             [~U[2022-01-01 12:00:00Z], ~U[2022-01-01 12:00:00.123Z]]
           ).rows == [
             [
               ~N[2022-01-01 12:00:00],
               "2022-01-01 13:00:00",
               ~N[2022-01-01 12:00:00.123],
               "2022-01-01 13:00:00.123"
             ]
           ]
  end
end
