defmodule Ch.Native do
  @moduledoc """
  Helpers for working with ClickHouse [`Native`](https://clickhouse.com/docs/en/sql-reference/formats#native) format.
  """

  @opaque buffer :: list()

  @spec new_buffer([String.t()]) :: buffer
  def new_buffer(types) do
    encoding_types = Ch.RowBinary.encoding_types(types)
    Enum.map(encoding_types, fn t -> [t | _column = []] end)
  end

  @spec add_row(buffer, [term]) :: buffer
  def add_row(buffer, row)

  def add_row([[type | column] | buffer_rest], [value | row_rest]) do
    [[type | [column | Ch.RowBinary.encode(type, value)]] | add_row(buffer_rest, row_rest)]
  end

  def add_row([] = done, []), do: done

  @spec to_iodata(buffer) :: iodata
  def to_iodata(buffer) do
    Enum.map(buffer, fn [_type | column] -> column end)
  end
end
