IO.puts("""
This benchmark measures the performance of decoding rows in RowBinary format.
""")

alias Ch.Bench.RowBinaryBenchmarkData
alias Ch.RowBinary

types = RowBinaryBenchmarkData.types()
decoded_types = RowBinary.decoding_types(types)

benchmark_output_path =
  System.get_env("BENCHMARK_OUTPUT_PATH", "bench/output/decode-github-action-benchmark.json")

Benchee.run(
  %{
    "RowBinary.decode_rows" => fn encoded ->
      RowBinary.decode_rows(encoded, types)
    end
  },
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" =>
      RowBinaryBenchmarkData.encoded_rows(1_000_000)
  },
  formatters: [
    Benchee.Formatters.Console,
    {GitHubActionBenchmarkFormatter,
     file: benchmark_output_path, suite_name: "Ch RowBinary Decode"}
  ]
)
