defmodule Ch do
  @moduledoc "Minimal HTTP ClickHouse client"
  alias Ch.{Connection, Query, Result, Health}

  def start_link(opts \\ []) do
    if many_endpoints?(opts) do
      children = [Health, DBConnection.child_spec(Connection, opts)]
      Supervisor.start_link(children, strategy: :one_for_one)
    else
      DBConnection.start_link(Connection, opts)
    end
  end

  @spec child_spec(opts :: Keywort.t()) :: :supervisor.child_spec()
  def child_spec(opts) do
    if many_endpoints?(opts) do
      children = [Health, DBConnection.child_spec(Connection, opts)]

      %{
        id: __MODULE__,
        start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
        type: :supervisor
      }
    else
      DBConnection.child_spec(Connection, opts)
    end
  end

  defp many_endpoints?(opts) do
    case Connection.endpoints(opts) do
      [_endpoint] -> false
      [_ | _] -> true
    end
  end

  @spec query(DBConnection.conn(), iodata, {:raw, iodata} | Enumerable.t(), Keyword.t()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))

    with {:ok, _query, result} <- DBConnection.execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  @spec query!(DBConnection.conn(), iodata, {:raw, iodata} | Enumerable.t(), Keyword.t()) ::
          Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))
    DBConnection.execute!(conn, query, params, opts)
  end
end
