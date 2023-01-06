defmodule Ch do
  @moduledoc "Minimal HTTP ClickHouse client."

  def start_link(opts \\ []) do
    DBConnection.start_link(Ch.Connection, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(Ch.Connection, opts)
  end

  def query(conn, statement, params \\ [], opts \\ []) do
    query = Ch.Query.build(statement, opts)

    with {:ok, _query, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Ch.Query.build(statement, opts)
    {_query, result} = DBConnection.prepare_execute!(conn, query, params, opts)
    result
  end

  def query_decode(conn, statement, params \\ [], opts \\ []) do
    with {:ok, result} <- query(conn, statement, params, opts) do
      %{data: data, headers: headers} = result
      {"x-clickhouse-format", format} = List.keyfind!(headers, "x-clickhouse-format", 0)
      {:ok, decode(format, data, opts)}
    end
  end

  defp decode("RowBinary", data, opts) do
    types = opts[:types] || raise ArgumentError, "missing :types"
    decode_row_binary(data, types)
  end

  defp decode("RowBinaryWithNamesAndTypes", data, _opts) do
    Ch.Protocol.decode_rows(data)
  end

  defp decode(format, _data, _opts) do
    raise ArgumentError, "automatic decoding for #{format} is not supported"
  end

  @doc """
  Encodes `RowBinary`

      iex> encode_row_binary([2], [:u32])
      _iodata = [<<2, 0, 0, 0>>]

  """
  def encode_row_binary(row, types), do: Ch.Protocol.encode_row(row, types)

  @doc """
  Encodes `RowBinary`

      iex> encode_row_binary_chunk([[2], [3]], [:u32])
      _iodata = [<<2, 0, 0, 0>>, <<3, 0, 0, 0>>]

  """
  def encode_row_binary_chunk(rows, types), do: Ch.Protocol.encode_rows(rows, types)

  @doc """
  Decodes `RowBinary`

      iex> decode_row_binary(<<2, 0, 0, 0>>, [:u32])
      _rows = [ _row = [2]]

  """
  def decode_row_binary(data, types), do: Ch.Protocol.decode_rows(data, types)
end
