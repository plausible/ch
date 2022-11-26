defmodule Ch do
  @moduledoc File.read!("README.md")

  def start_link(opts \\ []) do
    DBConnection.start_link(Ch.Protocol, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(Ch.Protocol, opts)
  end

  def query(conn, statement, params \\ [], opts \\ []) do
    IO.inspect([pid: self()], label: "Ch.query")
    query = Ch.Query.build(statement)

    with {:ok, _query, result} <- DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Ch.Query.build(statement)
    {_query, result} = DBConnection.prepare_execute!(conn, query, params, opts)
    result
  end
end
