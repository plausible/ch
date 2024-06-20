types = [Ch.Types.u64(), Ch.Types.string(), Ch.Types.array(Ch.Types.u8()), Ch.Types.datetime()]
encoding_types = Ch.RowBinary.encoding_types(types)

rows = fn count ->
  Enum.map(1..count, fn i ->
    [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], NaiveDateTime.utc_now()]
  end)
end

alias Ch.{RowBinary, Native}

defmodule NativeBuffer do
  def encode_row(row, buffer) do
    Native.add_row(buffer, row)
  end

  def encode_rows(rows, types) do
    rows
    |> Enum.reduce(Native.new_buffer(types), &__MODULE__.encode_row/2)
    |> Native.to_iodata()
  end
end

Benchee.run(
  %{
    "control" => fn rows -> Enum.each(rows, fn _row -> [] end) end,
    "RowBinary.encode_rows/2" => fn rows -> RowBinary.encode_rows(rows, types) end,
    "RowBinary._encode_rows/2" => fn rows -> RowBinary._encode_rows(rows, encoding_types) end,
    "Native.add_row/2" => fn rows -> NativeBuffer.encode_rows(rows, types) end
  },
  inputs: %{
    # TODO more inputs (take some from Plausible write buffer)
    "1_000_000 rows" => rows.(1_000_000)
  }
)
