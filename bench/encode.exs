IO.puts("""
This benchmark measures the performance of encoding rows in RowBinary format.
""")

defmodule Bench do
  def now([row | rows]) do
    row = Enum.map([:col1, :col2, :col3, :col4], fn key -> Map.fetch!(row, key) end)
    encoded = Ch.RowBinary._encode_row(row, [:u64, :string, {:array, :u8}, :datetime])
    [encoded | now(rows)]
  end

  def now([] = empty), do: empty

  def now_tail(rows), do: now_tail(rows, [])

  def now_tail([row | rows], acc) do
    row = Enum.map([:col1, :col2, :col3, :col4], fn key -> Map.fetch!(row, key) end)
    encoded = Ch.RowBinary._encode_row(row, [:u64, :string, {:array, :u8}, :datetime])
    now_tail(rows, [acc | encoded])
  end

  def now_tail([], acc), do: acc

  def next(rows), do: next(rows, [])

  defp next([%{col1: col1, col2: col2, col3: col3, col4: col4} | rest], acc) do
    encoded = [
      <<
        col1::64-unsigned-integer,
        to_unix(col4)::64-unsigned-integer
      >>,
      length(col3),
      col3,
      byte_size(col2),
      col2
    ]

    next(rest, [acc | encoded])
  end

  defp next([], acc), do: acc

  %Date{year: year, month: month} = Date.utc_today()
  new_epoch = DateTime.to_unix(DateTime.new!(Date.new!(year, month, 1), Time.new!(0, 0, 0)))

  defp to_unix(%DateTime{
         year: unquote(year),
         month: unquote(month),
         day: day,
         hour: hour,
         minute: minute,
         second: second
       }) do
    unquote(new_epoch) + (day - 1) * 86400 + hour * 3600 + minute * 60 + second
  end
end

Benchee.run(
  %{
    "now" => &Bench.now/1,
    "now_tail" => &Bench.now_tail/1,
    "next" => &Bench.next/1
  },
  profile_after: true,
  memory_time: 2,
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime) rows" =>
      Enum.map(1..1_000_000, fn i ->
        %{
          col1: i,
          col2: "Golang SQL database driver",
          col3: [1, 2, 3, 4, 5, 6, 7, 8, 9],
          col4: DateTime.utc_now()
        }
      end)
  }
)
