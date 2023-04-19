defmodule Ch do
  @moduledoc "Minimal HTTP ClickHouse client."
  alias Ch.{Connection, Query, Result}

  @doc """
  Start the connection process and connect to ClickHouse.

  ## Options

    * `:hostname` - server hostname, defaults to `"localhost"`
    * `:port` - HTTP port, defualts to `8123`
    * `:scheme` - HTTP scheme, defaults to `"http"`
    * `:database` - Database, defaults to `"default"`
    * `:username` - Username
    * `:password` - User password
    * `:settings` - Keyword list of ClickHouse settings
    * `:timeout` - HTTP receive timeout in milliseconds
    * `:transport_opts` - options to be given to the transport being used. See `Mint.HTTP1.connect/4` for more info

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
  `{:error, Exception.t()}` if there was a database error.

  ## Options

    * `:timeout` - Query request timeout
    * `:settings` - Keyword list of settings
    * `:database` - Database
    * `:username` - Username
    * `:password` - User password

  """
  @spec query(DBConnection.conn(), iodata, params, Keyword.t()) ::
          {:ok, Result.t()} | {:error, Exception.t()}
        when raw: iodata | Enumerable.t(), params: map | [term] | [row :: [term]] | {:raw, raw}
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
  @spec query!(DBConnection.conn(), iodata, params, Keyword.t()) :: Result.t()
        when raw: iodata | Enumerable.t(), params: map | [term] | [row :: [term]] | {:raw, raw}
  def query!(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))
    DBConnection.execute!(conn, query, params, opts)
  end

  @doc false
  @spec stream(DBConnection.t(), iodata, map | [term], Keyword.t()) :: DBConnection.Stream.t()
  def stream(conn, statement, params \\ [], opts \\ []) do
    query = Query.build(statement, Keyword.get(opts, :command))
    DBConnection.stream(conn, query, params, opts)
  end

  @doc false
  @spec run(DBConnection.conn(), (DBConnection.t() -> any), Keyword.t()) :: any
  def run(conn, f, opts \\ []) when is_function(f, 1) do
    DBConnection.run(conn, f, opts)
  end

  # if Code.ensure_loaded?(Ecto.ParametirizedType) do
  #   @behaviour Ecto.ParametirizedType

  #   @impl true
  #   def type(params), do: {:parametirized, Ch, params}

  #   @impl true
  #   def init(opts) do
  #     clickhouse_type = Keyword.fetch!(opts, :type)
  #     Ch.Types.decode(clickhouse_type)
  #   end

  #   @impl true
  #   def load(value, params), do: Ecto.Type.load(base_type(params), value)

  #   @impl true
  #   def dump(value, params), do: Ecto.Type.dump(base_type(params), value)

  #   @impl true
  #   def cast(value, params), do: Ecto.Type.cast(base_type(params), value)

  #   @impl true
  #   def embed_as(_, _), do: :self

  #   @impl true
  #   def equal?(a, b, _), do: a == b
  # end
end
