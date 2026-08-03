row_count =
  case System.get_env("ROWS") do
    nil -> 1_000_000
    value -> String.to_integer(value)
  end

iterations = String.to_integer(System.get_env("ITERATIONS", "3"))

schema = [
  id: "UInt64",
  title: "String",
  bytes: "Array(UInt8)",
  timestamp64: "DateTime64(3, 'UTC')",
  inserted_at: "DateTime"
]

suffix = System.unique_integer([:positive])
empty_module = "Ch.Bench.EmptyEncoder#{suffix}"
encoder_module = "Ch.Bench.GeneratedEncoder#{suffix}"
fixed_module = "Ch.Bench.FixedEncoder#{suffix}"

empty_source = "defmodule #{empty_module}, do: nil"

encoder_source = """
defmodule #{encoder_module} do
  require Ch.RowBinary.Encoder

  Ch.RowBinary.Encoder.define_encoder(
    name: :encode_lists,
    schema: #{inspect(schema)},
    row: :list,
    rows: true
  )

  Ch.RowBinary.Encoder.define_encoder(
    name: :encode_maps,
    schema: #{inspect(schema)},
    rows: true
  )
end
"""

fixed_source = """
defmodule #{fixed_module} do
  require Ch.RowBinary.Encoder

  Ch.RowBinary.Encoder.define_encoder(
    name: :encode_lists,
    schema: [id: "UInt64", small: "UInt8", active: "Bool", inserted_at: "DateTime"],
    row: :list,
    rows: true
  )
end
"""

{empty_compile_us, [{empty, empty_beam}]} = :timer.tc(fn -> Code.compile_string(empty_source) end)

{encoder_compile_us, [{encoder, encoder_beam}]} =
  :timer.tc(fn -> Code.compile_string(encoder_source) end)

{fixed_compile_us, [{fixed_encoder, fixed_beam}]} =
  :timer.tc(fn -> Code.compile_string(fixed_source) end)

titles = ["Golang SQL database driver", "Phoenix app event", "billing webhook payload"]
bytes = Enum.to_list(1..16)
base_datetime = DateTime.from_naive!(~N[2026-01-01 00:00:00.000], "Etc/UTC")
base_naive = ~N[2026-01-01 00:00:00]

rows =
  Enum.map(1..row_count, fn i ->
    [
      i,
      Enum.at(titles, rem(i, length(titles))),
      Enum.take(bytes, rem(i, length(bytes)) + 1),
      DateTime.add(base_datetime, i * 17, :millisecond),
      NaiveDateTime.add(base_naive, i, :second)
    ]
  end)

row_maps =
  Enum.map(rows, fn [id, title, bytes, timestamp64, inserted_at] ->
    %{
      id: id,
      title: title,
      bytes: bytes,
      timestamp64: timestamp64,
      inserted_at: inserted_at
    }
  end)

fixed_rows =
  Enum.map(rows, fn [id, _title, _bytes, _timestamp64, inserted_at] ->
    [id, rem(id, 256), rem(id, 2) == 0, inserted_at]
  end)

types = Keyword.values(schema)
fields = Keyword.keys(schema)
plan = Ch.RowBinary.prepare(types)
fixed_plan = Ch.RowBinary.prepare(["UInt64", "UInt8", "Bool", "DateTime"])

measure = fn name, function ->
  samples =
    Enum.map(1..iterations, fn _iteration ->
      :erlang.garbage_collect()
      {microseconds, result} = :timer.tc(function)
      # Observe the result so the benchmark includes completed encoder work.
      :erlang.phash2(result)
      microseconds / 1_000
    end)

  median = samples |> Enum.sort() |> Enum.at(div(iterations, 2))
  formatted_samples = Enum.map_join(samples, ", ", &to_string(Float.round(&1, 2)))
  IO.puts("#{name}: #{Float.round(median, 2)} ms median [#{formatted_samples}]")
end

IO.puts("Rows: #{row_count}")
IO.puts("Iterations: #{iterations}")
IO.puts("Empty module compile: #{Float.round(empty_compile_us / 1_000, 2)} ms")
IO.puts("Generated module compile: #{Float.round(encoder_compile_us / 1_000, 2)} ms")
IO.puts("Fixed module compile: #{Float.round(fixed_compile_us / 1_000, 2)} ms")
IO.puts("Empty module BEAM: #{byte_size(empty_beam)} bytes (#{inspect(empty)})")
IO.puts("Generated module BEAM: #{byte_size(encoder_beam)} bytes (#{inspect(encoder)})")
IO.puts("Generated BEAM delta: #{byte_size(encoder_beam) - byte_size(empty_beam)} bytes")
IO.puts("Fixed module BEAM: #{byte_size(fixed_beam)} bytes (#{inspect(fixed_encoder)})")
IO.puts("Fixed BEAM delta: #{byte_size(fixed_beam) - byte_size(empty_beam)} bytes")

measure.("Generated fixed-width list rows", fn ->
  apply(fixed_encoder, :encode_lists, [fixed_rows])
end)

measure.("Generic prepared fixed-width list rows", fn ->
  Ch.RowBinary.encode_rows(fixed_rows, fixed_plan)
end)

measure.("Generated list rows", fn -> apply(encoder, :encode_lists, [rows]) end)
measure.("Generic prepared list rows", fn -> Ch.RowBinary.encode_rows(rows, plan) end)

measure.("Generated atom maps", fn -> apply(encoder, :encode_maps, [row_maps]) end)

measure.("Generic atom maps via fields", fn ->
  generic_rows = Enum.map(row_maps, fn row -> Enum.map(fields, &Map.fetch!(row, &1)) end)
  Ch.RowBinary.encode_rows(generic_rows, plan)
end)
