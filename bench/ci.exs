# CI benchmark suite — no external services required.
# Tracks RowBinary encode/decode performance over time via BencheeGithub + github-action-benchmark.
# Run with: MIX_ENV=bench mix run bench/ci.exs

alias Ch.RowBinary

types = ["UInt64", "String", "Array(UInt8)", "DateTime"]
names = ["id", "name", "tags", "created_at"]

rows =
  Enum.map(1..1_000, fn i ->
    [i, "Golang SQL database driver", [1, 2, 3, 4, 5, 6, 7, 8, 9], DateTime.utc_now()]
  end)

encoded = IO.iodata_to_binary(RowBinary.encode_rows(rows, types))
encoded_with_header =
  IO.iodata_to_binary([RowBinary.encode_names_and_types(names, types) | encoded])

Benchee.run(
  %{
    "encode_rows/2 — 1_000 rows" => fn -> RowBinary.encode_rows(rows, types) end,
    "decode_rows/2 — 1_000 rows" => fn -> RowBinary.decode_rows(encoded, types) end,
    "decode_names_and_rows/1 — 1_000 rows" => fn ->
      RowBinary.decode_names_and_rows(encoded_with_header)
    end
  },
  formatters: [Benchee.Formatters.Console, {BencheeGithub, output_path: "bench_output.json"}]
)

