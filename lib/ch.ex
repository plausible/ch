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

  @doc """
  Helper function that uses `FORMAT RowBinaryWithNamesAndTypes` and automatically decodes the response
  """
  def query_rows(conn, statement, params \\ [], opts \\ []) do
    # TODO
    query =
      Ch.Query.build(
        [statement | " FORMAT RowBinaryWithNamesAndTypes"],
        opts[:command] || Ch.Query.extract_command(statement)
      )

    with {:ok, _query, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      rows = Ch.Protocol.decode_rows(result)
      {:ok, %{num_rows: length(rows), rows: rows}}
    end
  end

  @doc """
  Encodes `RowBinary`

      iex> encode_row_binary([2], [:u32])
      _iodata = [<<2, 0, 0, 0>>]

  """
  def encode_row_binary(row, types), do: Ch.Protocol.encode_row(row, types)

  @doc """
  Decodes `RowBinary`

      iex> decode_row_binary(<<2, 0, 0, 0>>, [:u32])
      _rows = [ _row = [2]]

  """
  def decode_row_binary(data, types), do: Ch.Protocol.decode_rows(data, types)
end
