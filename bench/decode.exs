IO.puts("""
This benchmark measures the performance of decoding rows in RowBinary format.
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
         file: System.fetch_env!("BENCHMARK_OUTPUT_PATH"), suite_name: "Ch RowBinary Decode"}
      end
    ],
    &is_nil/1
  )

Benchee.run(
  %{
    "RowBinary.decode_rows" => fn encoded ->
      RowBinary.decode_rows(encoded, types)
    end
  },
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      RowBinaryBenchmarkData.encoded_rows(1_000_000),
    "100_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      RowBinaryBenchmarkData.encoded_rows(100_000),
    "10_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      RowBinaryBenchmarkData.encoded_rows(10_000),
    "1000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      RowBinaryBenchmarkData.encoded_rows(1000)
  },
  formatters: formatters
)
