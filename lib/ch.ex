defmodule Ch do
  @moduledoc "Minimal HTTP ClickHouse client"
  alias Ch.{Connection, Query, Result}

  def start_link(opts \\ []) do
    DBConnection.start_link(Connection, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(Connection, opts)
  end

  @spec query(DBConnection.conn(), iodata, Enumerable.t() | iodata, Keyword.t()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))

    with {:ok, _query, result} <- DBConnection.execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  @spec query!(DBConnection.conn(), iodata, Enumerable.t() | iodata, Keyword.t()) :: Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))
    DBConnection.execute!(conn, query, params, opts)
  end
end
