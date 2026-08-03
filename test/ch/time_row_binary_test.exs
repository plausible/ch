defmodule Ch.TimeRowBinaryTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  property "Time values round-trip through RowBinary at second precision" do
    check all time <- time_gen() do
      expected = truncate_time(time, 0)

      assert [[^expected]] =
               time
               |> encode_one("Time")
               |> RowBinary.decode_rows(["Time"])
    end
  end

  property "Time64 values round-trip through RowBinary at their declared precision" do
    check all precision <- integer(0..9),
              time <- time_gen() do
      type = "Time64(#{precision})"
      expected = truncate_time(time, precision)

      assert [[^expected]] =
               time
               |> encode_one(type)
               |> RowBinary.decode_rows([type])
    end
  end

  test "Time64 encoder truncates fractional ticks for sub-microsecond precision" do
    time = ~T[12:34:56.987654]

    assert RowBinary.encode({:time64, 1}, time) ==
             <<12 * 60 * 60 + 34 * 60 + 56::64-little-signed>>

    assert RowBinary.encode({:time64, 10}, time) ==
             <<452_960 + 9::64-little-signed>>

    assert RowBinary.encode({:time64, 100}, time) ==
             <<4_529_600 + 98::64-little-signed>>

    assert RowBinary.encode({:time64, 1_000}, time) ==
             <<45_296_000 + 987::64-little-signed>>
  end

  test "Time64 decoder rejects ClickHouse values outside Elixir's Time range" do
    for precision <- 0..9 do
      time_unit = Integer.pow(10, precision)
      type = "Time64(#{precision})"

      assert_raise ArgumentError, ~r/out of Elixir's Time range/, fn ->
        RowBinary.decode_rows(<<-1::64-little-signed>>, [type])
      end

      assert_raise ArgumentError, ~r/out of Elixir's Time range/, fn ->
        RowBinary.decode_rows(<<86_400 * time_unit::64-little-signed>>, [type])
      end
    end
  end

  test "Time64 decoder matches Unix conversion across all precisions and boundaries" do
    for precision <- 0..9 do
      time_unit = Integer.pow(10, precision)
      type = "Time64(#{precision})"

      ticks = [
        0,
        1,
        time_unit - 1,
        time_unit,
        time_unit + 1,
        43_200 * time_unit + div(123_456_789 * time_unit, 1_000_000_000),
        86_400 * time_unit - 1
      ]

      for ticks <- ticks do
        expected = ticks |> DateTime.from_unix!(time_unit) |> DateTime.to_time()

        assert RowBinary.decode_rows(<<ticks::64-little-signed>>, [type]) == [[expected]],
               "precision #{precision}, ticks #{ticks}"
      end
    end
  end

  defp encode_one(value, type) do
    [[value]]
    |> RowBinary.encode_rows([type])
    |> IO.iodata_to_binary()
  end

  defp time_gen do
    gen all hour <- integer(0..23),
            minute <- integer(0..59),
            second <- integer(0..59),
            microsecond <- integer(0..999_999) do
      Time.new!(hour, minute, second, {microsecond, 6})
    end
  end

  defp truncate_time(time, precision) do
    {microsecond, _} = time.microsecond

    precision = min(precision, 6)

    microsecond =
      if precision == 6 do
        microsecond
      else
        scale = Integer.pow(10, 6 - precision)
        div(microsecond, scale) * scale
      end

    %{time | microsecond: {microsecond, precision}}
  end
end
