rowbinary = fn count ->
  Enum.map(1..count, fn i ->
    row = [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], DateTime.utc_now()]
    Ch.RowBinary.encode_row(row, ["UInt64", "String", "Array(UInt8)", "DateTime"])
  end)
end

Benchee.run(
  %{
    "zstd once" => fn input -> :zstd.compress(input) end,
    "zstd stream" => fn input -> Compress.zstd_stream(input) end,
    "nimble_lz4 once" => fn input -> NimbleLZ4.compress(input) end
  },
  inputs: %{
    "1 rows" => rowbinary.(1),
    "1000 rows" => rowbinary.(1000),
    "100,000 rows" => rowbinary.(100_000)
  }
)
