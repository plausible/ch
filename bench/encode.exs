IO.puts("""
This benchmark measures the performance of encoding rows in RowBinary format.
""")

alias Ch.RowBinary

types = ["UInt64", "String", "Array(UInt8)", "DateTime64(3, 'UTC')", "DateTime"]
titles = ["Golang SQL database driver", "Phoenix app event", "billing webhook payload"]
bytes = Enum.to_list(1..16)
base_datetime = DateTime.from_naive!(~N[2026-01-01 00:00:00.000], "Etc/UTC")
base_naive = ~N[2026-01-01 00:00:00]

benchmark_output_path =
  System.get_env("BENCHMARK_OUTPUT_PATH", "bench/output/encode-github-action-benchmark.json")

rows = fn count ->
  Enum.map(1..count, fn i ->
    [
      i,
      Enum.at(titles, rem(i, length(titles))),
      Enum.take(bytes, rem(i, length(bytes)) + 1),
      DateTime.add(base_datetime, i * 17, :millisecond),
      NaiveDateTime.add(base_naive, i, :second)
    ]
  end)
end

Benchee.run(
  %{
    "RowBinary" => fn rows -> RowBinary.encode_rows(rows, types) end,
    "RowBinary stream of 100k row chunks" => fn rows ->
      types = RowBinary.encoding_types(types)

      Stream.chunk_every(rows, 100_000)
      |> Stream.each(fn chunk -> RowBinary._encode_rows(chunk, types) end)
      |> Stream.run()
    end
  },
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      rows.(1_000_000)
  },
  warmup: 2,
  time: 8,
  memory_time: 0,
  formatters: [
    Benchee.Formatters.Console,
    {GitHubActionBenchmarkFormatter,
     file: benchmark_output_path, suite_name: "Ch RowBinary Encode"}
  ]
)
