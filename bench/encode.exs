IO.puts("""
This benchmark measures the performance of encoding rows in RowBinary format.
""")

alias Ch.RowBinary

types = ["UInt64", "String", "Array(UInt8)", "DateTime"]

rows = fn count ->
  Enum.map(1..count, fn i ->
    [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], DateTime.utc_now()]
  end)
end

Benchee.run(
  %{
    "RowBinary" => fn rows -> RowBinary.encode_rows(rows, types) end,
    "RowBinary stream" => fn rows ->
      Stream.chunk_every(rows, 60_000)
      |> Stream.each(fn chunk -> RowBinary.encode_rows(chunk, types) end)
      |> Stream.run()
    end
  },
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime) rows" => rows.(1_000_000)
  }
)
