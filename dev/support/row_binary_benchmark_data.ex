defmodule Ch.Bench.RowBinaryBenchmarkData do
  alias Ch.RowBinary

  @schema [
    id: "UInt64",
    title: "String",
    bytes: "Array(UInt8)",
    timestamp64: "DateTime64(3, 'UTC')",
    inserted_at: "DateTime"
  ]
  @fields Keyword.keys(@schema)
  @types Keyword.values(@schema)
  @titles ["Golang SQL database driver", "Phoenix app event", "billing webhook payload"]
  @bytes Enum.to_list(1..16)
  @base_datetime DateTime.from_naive!(~N[2026-01-01 00:00:00.000], "Etc/UTC")
  @base_naive ~N[2026-01-01 00:00:00]

  def schema, do: @schema
  def fields, do: @fields
  def types, do: @types

  def rows(count) do
    Enum.map(1..count, fn i ->
      [
        i,
        Enum.at(@titles, rem(i, length(@titles))),
        Enum.take(@bytes, rem(i, length(@bytes)) + 1),
        DateTime.add(@base_datetime, i * 17, :millisecond),
        NaiveDateTime.add(@base_naive, i, :second)
      ]
    end)
  end

  def row_maps(count) do
    Enum.map(1..count, fn i ->
      %{
        id: i,
        title: Enum.at(@titles, rem(i, length(@titles))),
        bytes: Enum.take(@bytes, rem(i, length(@bytes)) + 1),
        timestamp64: DateTime.add(@base_datetime, i * 17, :millisecond),
        inserted_at: NaiveDateTime.add(@base_naive, i, :second)
      }
    end)
  end

  def encoded_rows(count) do
    count
    |> rows()
    |> RowBinary.encode_rows(@types)
    |> IO.iodata_to_binary()
  end
end
