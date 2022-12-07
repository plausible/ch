defmodule Ch do
  @moduledoc File.read!("README.md")

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

  def encode_row(row, types), do: Ch.Protocol.encode_row(row, types)
end
