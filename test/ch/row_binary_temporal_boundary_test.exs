defmodule Ch.RowBinaryTemporalBoundaryTest do
  use ExUnit.Case, async: true

  alias Ch.RowBinary

  import Bitwise

  @epoch ~D[1970-01-01]

  test "Date accepts its UInt16 boundaries without wrapping" do
    max_date = Date.add(@epoch, (1 <<< 16) - 1)

    assert RowBinary.decode_rows(encoded(:date, @epoch), [:date]) == [[@epoch]]
    assert RowBinary.decode_rows(encoded(:date, max_date), [:date]) == [[max_date]]

    for date <- [Date.add(@epoch, -1), Date.add(max_date, 1)] do
      assert_raise ArgumentError, ~r/out of range/, fn -> RowBinary.encode(:date, date) end
    end
  end

  test "DateTime accepts its UInt32 boundaries without wrapping" do
    max_seconds = (1 <<< 32) - 1
    epoch = DateTime.from_unix!(0)
    max_datetime = DateTime.from_unix!(max_seconds)

    assert RowBinary.decode_rows(encoded(:datetime, epoch), [:datetime]) ==
             [[DateTime.to_naive(epoch)]]

    assert RowBinary.decode_rows(encoded(:datetime, max_datetime), [:datetime]) ==
             [[DateTime.to_naive(max_datetime)]]

    for datetime <- [DateTime.from_unix!(-1), DateTime.from_unix!(max_seconds + 1)] do
      message =
        ~r/cannot encode .* as DateTime since it's (before Unix epoch|after the maximum Unix timestamp)/

      assert_raise ArgumentError, message, fn ->
        RowBinary.encode(:datetime, datetime)
      end

      assert_raise ArgumentError, message, fn ->
        RowBinary.encode(:datetime, DateTime.to_naive(datetime))
      end
    end
  end

  test "DateTime64 rejects tick counts outside Int64" do
    datetime = ~U[9999-12-31 23:59:59.999999Z]

    assert_raise ArgumentError, ~r/out of range/, fn ->
      RowBinary.encode({:datetime64, 1_000_000_000}, datetime)
    end

    assert_raise ArgumentError, ~r/out of range/, fn ->
      RowBinary.encode({:datetime64, 1_000_000_000}, DateTime.to_naive(datetime))
    end
  end

  defp encoded(type, value) do
    type |> RowBinary.encode(value) |> IO.iodata_to_binary()
  end
end
