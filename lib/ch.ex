defmodule Ch do
  @moduledoc "Minimal HTTP ClickHouse client"
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

  for size <- [8, 16, 32, 64, 128, 256] do
    @doc "UInt#{size} type helper"
    def unquote(:"u#{size}")(), do: unquote(:"u#{size}")
    @doc "Int#{size} type helper"
    def unquote(:"i#{size}")(), do: unquote(:"i#{size}")
  end

  for size <- [32, 64] do
    @doc "Float#{size} type helper"
    def unquote(:"f#{size}")(), do: unquote(:"f#{size}")
  end

  @doc "UTF8 String type helper"
  def string, do: :string
  @doc "Binary type helper"
  def binary, do: :binary
  @doc "Boolean type helper"
  def boolean, do: :boolean
  @doc "Date type helper"
  def date, do: :date
  @doc "DateTime type helper"
  def datetime, do: :datetime
  @doc "Date32 type helper"
  def date32, do: :date32
  @doc "DateTime64(precision) type helper"
  def datetime64(unit), do: {:datetime64, unit, nil}
  @doc "UUID type helper"
  def uuid, do: :uuid

  # def datetime(tz) when is_binary(tz), do: {:datetime, tz}
  # def datetime64(unit, timezone), do: {:datetime64, unit, nil}

  @doc "FixedString(size) type helper"
  def string(size) when is_integer(size) and size > 0, do: {:string, size}

  for size <- [32, 64, 128, 256] do
    @doc "Decimal#{size}(scale) type helper"
    def unquote(:"decimal#{size}")(scale) when is_integer(scale) do
      {:decimal, unquote(size), scale}
    end
  end

  @doc "Array(T) type helper"
  def array(type), do: {:array, type}
  @doc "Nullable(T) type helper"
  def nullable(type), do: {:nullable, type}
end
