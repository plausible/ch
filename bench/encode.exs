IO.puts("""
This benchmark measures the performance of encoding rows in RowBinary format.
""")

alias Ch.Bench.RowBinaryBenchmarkData
alias Ch.RowBinary

types = RowBinaryBenchmarkData.types()

formatters =
  Enum.reject(
    [
      Benchee.Formatters.Console,
      if System.get_env("CI") do
        {GitHubActionBenchmarkFormatter,
         file: System.fetch_env!("BENCHMARK_OUTPUT_PATH"), suite_name: "Ch RowBinary Encode"}
      end
    ],
    &is_nil/1
  )

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
      RowBinaryBenchmarkData.rows(1_000_000)
  },
  formatters: formatters
)
