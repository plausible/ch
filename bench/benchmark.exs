alias Ch.RowBinary

Code.require_file("json_formatter.exs", __DIR__)

types = ["UInt64", "String", "Array(UInt8)", "DateTime64(3, 'UTC')", "DateTime"]
titles = ["Golang SQL database driver", "Phoenix app event", "billing webhook payload"]
bytes = Enum.to_list(1..16)
base_datetime = DateTime.from_naive!(~N[2026-01-01 00:00:00.000], "Etc/UTC")
base_naive = ~N[2026-01-01 00:00:00]

make_rows = fn count ->
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

inputs =
  for count <- [1_000, 10_000], into: %{} do
    rows = make_rows.(count)

    {"#{count} rows",
     %{rows: rows, encoded: rows |> RowBinary.encode_rows(types) |> IO.iodata_to_binary()}}
  end

version =
  case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
    {sha, 0} -> String.trim(sha)
    _ -> "working-tree"
  end

github_actions? = System.get_env("GITHUB_ACTIONS") == "true"
profile_after = if github_actions? or "--profile" in System.argv(), do: :tprof, else: false

Benchee.run(
  %{
    "RowBinary.encode_rows/2" => fn %{rows: rows} -> RowBinary.encode_rows(rows, types) end,
    "RowBinary.decode_rows/2" => fn %{encoded: encoded} ->
      RowBinary.decode_rows(encoded, types)
    end
  },
  inputs: inputs,
  time: 3,
  warmup: 1,
  max_sample_size: 20_000,
  measure_function_call_overhead: true,
  pre_check: true,
  profile_after: profile_after,
  formatters: [
    {Benchee.Formatters.Console, extended_statistics: true},
    {Ch.Bench.JSONFormatter,
     output_root: "bench/output",
     benchmark_version: version,
     machine_prefix: if(github_actions?, do: "github-actions", else: "local")}
  ]
)
