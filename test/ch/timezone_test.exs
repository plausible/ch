defmodule Ch.TimezoneTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  import Ch.Test, only: [parameterize_query!: 2, parameterize_query!: 3]

  test "session timezone is reported by ClickHouse", ctx do
    ctx = setup_conn(ctx, "Asia/Taipei")
    result = parameterize_query!(ctx, "SELECT timezone()")

    # The session setting changes both timezone() and the timezone response header, just like
    # configuring the ClickHouse server timezone does.
    assert result.rows == [["Asia/Taipei"]]

    assert List.keyfind!(result.headers, "x-clickhouse-timezone", 0) ==
             {"x-clickhouse-timezone", "Asia/Taipei"}
  end

  test "naive datetime params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Europe/Berlin")

    # Naive datetimes are sent as text, so ClickHouse interprets them in the session timezone
    # before Ch decodes the underlying instant as a naive UTC datetime.
    assert parameterize_query!(
             ctx,
             "SELECT {naive:DateTime} AS d, toString(d)",
             %{"naive" => ~N[2022-01-01 12:00:00]}
           ).rows == [[~N[2022-01-01 11:00:00], "2022-01-01 12:00:00"]]

    # Cover the positional parameter path and verify that daylight-saving time is observed.
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime} AS d, toString(d)",
             [~N[2022-07-01 12:00:00]]
           ).rows == [[~N[2022-07-01 10:00:00], "2022-07-01 12:00:00"]]
  end

  test "naive datetime64 params use the session timezone", ctx do
    ctx = setup_conn(ctx, "Asia/Tokyo")

    # Preserve coverage for every supported DateTime64 precision from the former integration test.
    for precision <- 0..9 do
      assert [[datetime]] =
               parameterize_query!(
                 ctx,
                 "SELECT {$0:DateTime64(#{precision})}",
                 [~N[2022-01-01 12:00:00]]
               ).rows

      assert NaiveDateTime.compare(datetime, ~N[2022-01-01 03:00:00]) == :eq
    end

    # A fractional value also verifies that timezone conversion retains subsecond precision.
    assert parameterize_query!(
             ctx,
             "SELECT {$0:DateTime64(3)} AS d, toString(d)",
             [~N[2022-01-01 12:00:00.123]]
           ).rows == [[~N[2022-01-01 03:00:00.123], "2022-01-01 12:00:00.123"]]
  end

  test "UTC datetime params keep their instant and render in the session timezone", ctx do
    ctx = setup_conn(ctx, "Australia/Sydney")

    # UTC DateTimes are encoded as instants; the session timezone only affects their text rendering.
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
