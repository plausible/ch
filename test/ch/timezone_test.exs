defmodule Ch.TimezoneTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  import Ch.Test, only: [parameterize_query!: 2, parameterize_query!: 3]

  test "session timezone is reported by ClickHouse", ctx do
    ctx = setup_conn(ctx, "Asia/Taipei")
    result = parameterize_query!(ctx, "SELECT timezone()")

    assert result.rows == [["Asia/Taipei"]]

    assert List.keyfind!(result.headers, "x-clickhouse-timezone", 0) ==
             {"x-clickhouse-timezone", "Asia/Taipei"}
  end

  test "naive datetime params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Europe/Berlin")

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
    ctx = setup_conn(ctx, "Asia/Tokyo")

    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime64(3)} AS d, toString(d)",
             [~N[2022-01-01 12:00:00.123]]
           ).rows == [[~N[2022-01-01 03:00:00.123], "2022-01-01 12:00:00.123"]]
  end

  test "UTC datetime params keep their instant and render in the session timezone", ctx do
    ctx = setup_conn(ctx, "Australia/Sydney")

    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d), {$1:DateTime64(3)} AS d64, toString(d64)",
             [~U[2022-01-01 12:00:00Z], ~U[2022-01-01 12:00:00.123Z]]
           ).rows == [
             [
               ~N[2022-01-01 12:00:00],
               "2022-01-01 23:00:00",
               ~N[2022-01-01 12:00:00.123],
               "2022-01-01 23:00:00.123"
             ]
           ]
  end

  defp setup_conn(ctx, timezone) do
    conn =
      start_supervised!(
        {Ch, database: Ch.Test.database(), settings: [session_timezone: timezone]}
      )

    Map.put(ctx, :conn, conn)
  end
end
