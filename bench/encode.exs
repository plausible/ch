IO.puts("""
This benchmark measures the performance of encoding rows in RowBinary format.
""")

alias Ch.Bench.RowBinaryBenchmarkData
alias Ch.RowBinary

defmodule PredefinedRowBinaryEncoder do
  require Ch.RowBinary

  Ch.RowBinary.define_encoder(
    schema: Ch.Bench.RowBinaryBenchmarkData.schema(),
    name: :encode_many,
    rows: true
  )

  Ch.RowBinary.define_encoder(
    schema: Ch.Bench.RowBinaryBenchmarkData.schema(),
    name: :insert_many,
    table: "benchmark"
  )
end

types = RowBinaryBenchmarkData.types()
fields = RowBinaryBenchmarkData.fields()
encoding_types = RowBinary.encoding_types(types)

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
    "RowBinary row lists" => fn %{rows: rows} ->
      RowBinary._encode_rows(rows, encoding_types)
    end,
    "RowBinary atom maps via fields" => fn %{row_maps: row_maps} ->
      rows =
        Enum.map(row_maps, fn row ->
          Enum.map(fields, fn field -> Map.fetch!(row, field) end)
        end)

      RowBinary._encode_rows(rows, encoding_types)
    end,
    "Predefined atom map encoder" => fn %{row_maps: row_maps} ->
      PredefinedRowBinaryEncoder.encode_many(row_maps)
    end,
    "Predefined full insert body" => fn %{row_maps: row_maps} ->
      PredefinedRowBinaryEncoder.insert_many(row_maps)
    end,
    "Predefined atom map encoder stream of 100k row chunks" => fn %{row_maps: row_maps} ->
      Stream.chunk_every(row_maps, 100_000)
      |> Stream.each(fn chunk -> PredefinedRowBinaryEncoder.encode_many(chunk) end)
      |> Stream.run()
    end
  },
  before_scenario: fn count ->
    %{rows: RowBinaryBenchmarkData.rows(count), row_maps: RowBinaryBenchmarkData.row_maps(count)}
  end,
  inputs: %{
    "1_000_000 (UInt64, String, Array(UInt8), DateTime64(3, 'UTC'), DateTime) rows" => 1_000_000
  },
  formatters: formatters
)
