defmodule Ch do
  @moduledoc """
  ClickHouse driver for Elixir.

  Ch uses HTTP interface and `RowBinaryWithTypesAndNames` format
  for all statements except for `INSERT`. In order to enable custom formats,
  `INSERT` statements require the parameters to be encoded prior to calling `query/4`.
  """
  alias Ch.{Connection, Query, Result}

  @doc """
  Start the connection process and connect to ClickHouse.

  ## Options

    * `:hostname` - server hostname, defaults to `localhost`
    * `:port` - HTTP port, defualts to `8123`
    * `:scheme` - HTTP scheme, defaults to `"http"`
    * `:database` - Database, defaults to `"default"`
    * `:username` - Username
    * `:password` - User password
    * `:settings` - Keyword list of settings
    * `:timeout` - HTTP receive timeout in milliseconds

  """
  def start_link(opts \\ []) do
    DBConnection.start_link(Connection, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  def child_spec(opts) do
    DBConnection.child_spec(Connection, opts)
  end

  @doc """
  Runs a query and returns the result as `{:ok, %Ch.Result{}}` or
  `{:error, %Ch.Error{}}` if there was a database error.

  ## Options

    * `:timeout` - Query request timeout
    * `:settings` - Keyword list of settings
    * `:database` - Database
    * `:username` - Username
    * `:password` - User password

  """
  @spec query(DBConnection.conn(), iodata, {:raw, iodata} | Enumerable.t(), Keyword.t()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))

    with {:ok, _query, result} <- DBConnection.execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  @doc """
  Runs a query and returns the result or raises `Ch.Error` if
  there was an error. See `query/4`.
  """
  @spec query!(DBConnection.conn(), iodata, {:raw, iodata} | Enumerable.t(), Keyword.t()) ::
          Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))
    DBConnection.execute!(conn, query, params, opts)
  end
end
