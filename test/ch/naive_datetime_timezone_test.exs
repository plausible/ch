defmodule Ch.NaiveDateTimeTimezoneTest do
  use ExUnit.Case, async: true

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  test "naive DateTime params use session_timezone for implicit timezone types", %{pool: pool} do
    naive = ~N[2022-12-12 12:00:00]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime} AS d, toString(d), timeZone()",
             %{"dt" => naive},
             settings: [session_timezone: "Asia/Bangkok"]
           ).rows == [[~N[2022-12-12 05:00:00], "2022-12-12 12:00:00", "Asia/Bangkok"]]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime} AS d, toString(d), timeZone()",
             %{"dt" => naive},
             settings: [session_timezone: "Europe/Berlin"]
           ).rows == [[~N[2022-12-12 11:00:00], "2022-12-12 12:00:00", "Europe/Berlin"]]
  end

  test "naive DateTime64 params use session_timezone for implicit timezone types", %{pool: pool} do
    naive = ~N[2022-12-12 12:00:00.123]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime64(3)} AS d, toString(d), timeZone()",
             %{"dt" => naive},
             settings: [session_timezone: "Asia/Bangkok"]
           ).rows == [[~N[2022-12-12 05:00:00.123], "2022-12-12 12:00:00.123", "Asia/Bangkok"]]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime64(3)} AS d, toString(d), timeZone()",
             %{"dt" => naive},
             settings: [session_timezone: "Europe/Berlin"]
           ).rows == [[~N[2022-12-12 11:00:00.123], "2022-12-12 12:00:00.123", "Europe/Berlin"]]
  end

  test "naive DateTime params with explicit timezone ignore session_timezone", %{pool: pool} do
    naive = ~N[2022-12-12 12:00:00]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime('Asia/Bangkok')} AS d, toString(d)",
             %{"dt" => naive},
             settings: [session_timezone: "UTC"]
           ).rows == [
             [
               DateTime.new!(~D[2022-12-12], ~T[12:00:00], "Asia/Bangkok"),
               "2022-12-12 12:00:00"
             ]
           ]
  end

  test "naive DateTime64 params with explicit timezone ignore session_timezone", %{pool: pool} do
    naive = ~N[2022-12-12 12:00:00.123]

    assert Ch.query!(
             pool,
             "SELECT {dt:DateTime64(3, 'Asia/Bangkok')} AS d, toString(d)",
             %{"dt" => naive},
             settings: [session_timezone: "UTC"]
           ).rows == [
             [
               DateTime.new!(~D[2022-12-12], ~T[12:00:00.123], "Asia/Bangkok"),
               "2022-12-12 12:00:00.123"
             ]
           ]
  end
end
