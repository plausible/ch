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

  if Code.ensure_loaded?(Ecto.ParameterizedType) do
    @behaviour Ecto.ParameterizedType

    @impl true
    def type(params), do: {:parameterized, Ch, params}

    @impl true
    def init(opts) do
      clickhouse_type = Keyword.fetch!(opts, :type)
      Ch.Types.decode(clickhouse_type)
    end

    @impl true
    def load(value, _loader, params), do: Ecto.Type.load(base_type(params), value)

    @impl true
    def dump(value, _dumper, params), do: Ecto.Type.dump(base_type(params), value)

    @impl true
    def cast(value, params), do: Ecto.Type.cast(base_type(params), value)

    @doc false
    def base_type(type)

    def base_type(t) when t in [:string, :boolean, :date], do: t
    def base_type(:date32), do: :date
    def base_type(:datetime), do: :naive_datetime
    # TODO
    def base_type({:enum8, _mappings}), do: :string
    def base_type({:enum16, _mappings}), do: :string

    # TODO
    def base_type(:ipv4), do: :integer
    def base_type(:ipv6), do: :string
    def base_type(:uuid), do: Ecto.UUID

    for size <- [8, 16, 32, 64, 128, 256] do
      def base_type(unquote(:"i#{size}")), do: :integer
      def base_type(unquote(:"u#{size}")), do: :integer
    end

    for size <- [32, 64] do
      def base_type(unquote(:"f#{size}")), do: :float
    end

    def base_type({:array, type}), do: {:array, base_type(type)}
    def base_type({:nullable, type}), do: base_type(type)
    def base_type({:low_cardinality, type}), do: base_type(type)
    def base_type({:simple_aggregate_function, _name, type}), do: base_type(type)
    def base_type({:fixed_string, _size}), do: :string
    def base_type({:datetime, _timezone}), do: :utc_datetime
    def base_type({:datetime64, _precision}), do: :naive_datetime_usec
    def base_type({:datetime64, _precision, _timezone}), do: :utc_datetime_usec
    def base_type({:decimal = d, _precision, _scale}), do: d

    for size <- [32, 64, 128, 256] do
      def base_type({unquote(:"decimal#{size}"), _scale}), do: :decimal
    end

    def base_type({:parameterized, Ch, params}), do: base_type(params)

    @impl true
    def embed_as(_, _), do: :self

    @impl true
    def equal?(a, b, _), do: a == b
  end
end
