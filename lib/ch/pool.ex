defmodule Ch.Pool do
  @moduledoc """
  Connection pool for ClickHouse HTTP requests using NimblePool and Mint.

  Connections are established lazily during checkout in the caller process.
  Idle connections close after 5 seconds by default (ClickHouse default `keep_alive_timeout` is 10 seconds).

  Queries default to the `RowBinaryWithNamesAndTypes` format.
  """

  @behaviour NimblePool

  @query_timeout to_timeout(second: 30)
  @default_format "RowBinaryWithNamesAndTypes"

  @query_headers [
    {"x-clickhouse-format", @default_format},
    {"user-agent", "ch/#{Mix.Project.config()[:version]}"}
  ]

  defmodule Connection do
    @moduledoc false
    defstruct [:conn, :pool]

    @type t :: %__MODULE__{conn: Mint.HTTP1.t(), pool: pid}
  end

  @typedoc """
  The query payload.

  This can be a standard SQL string or SQL appended with RowBinary data (`[sql, ?\n, rowbinary]`).
  If providing compressed payloads, pass the appropriate `content-encoding` header.
  """
  @type query_statement :: iodata

  @typedoc """
  Query parameters map mapped to ClickHouse parameters (e.g., `{a:UInt64}`).

  These are encoded directly into the URL query string and are subject to URL length limits.
  """
  @type query_params :: %{String.t() => term} | keyword

  # TODO add :strings :copy | :auto | etc.

  @typedoc """
  Query execution options.

  * `:timeout` - Request timeout, defaults to 30 seconds.
  * `:settings` - ClickHouse settings added to the URL query string.
  * `:query` - Extra raw URL query params. Kept as an alias/escape hatch.
  * `:headers` - Headers passed to Mint. User headers override defaults by header name.
  * `:decode` - Whether to decode `RowBinaryWithNamesAndTypes` responses, defaults to `true`.
  """
  @type query_option ::
          {:timeout, timeout}
          | {:settings, Enumerable.t()}
          | {:query, Enumerable.t()}
          | {:headers, Mint.Types.headers()}
          | {:decode, boolean}

  @typedoc """
  The parsed query response.

  If the format is `RowBinaryWithNamesAndTypes`, it returns `%{names: [name], rows: [[value]]}`.
  Otherwise, it returns the raw response body binary.
  """
  @type query_result :: %{names: [String.t()], rows: [[term]]} | binary

  @typedoc """
  A query execution error.

  Returns `Ch.Error` for ClickHouse errors or Mint errors for network/HTTP failures.
  """
  @type query_error :: Ch.Error.t() | Mint.Types.error()

  @start_options_schema [
    name: [
      type: :any,
      doc: "Process name registration (e.g. `MyPool` or `{:via, Registry, :ch}`)."
    ],
    pool_size: [
      type: :pos_integer,
      doc: "Maximum number of concurrent connections.",
      default: 20
    ],
    worker_idle_timeout: [
      type: :timeout,
      doc: """
      Time a connection can stay idle before the pool closes it.
      Should be lower than ClickHouse's `keep_alive_timeout`.
      """,
      default: to_timeout(second: 5)
    ],
    url: [
      type: :string,
      doc: "The ClickHouse endpoint URL.",
      default: "http://localhost:8123"
    ]
  ]

  @typedoc """
  The options supported by `start_link/1`.
  """
  @type start_option :: unquote(NimbleOptions.option_typespec(@start_options_schema))

  @doc """
  Starts a new Ch pool process.

  Supported options:
  #{NimbleOptions.docs(@start_options_schema)}
  """
  @spec start_link([start_option]) :: GenServer.on_start()
  def start_link(options \\ []) do
    options = NimbleOptions.validate!(options, @start_options_schema)

    name = Keyword.get(options, :name)
    pool_size = Keyword.fetch!(options, :pool_size)
    worker_idle_timeout = Keyword.fetch!(options, :worker_idle_timeout)
    url = Keyword.fetch!(options, :url)

    %URI{scheme: scheme, host: host, port: port} = URI.parse(url)

    scheme =
      case scheme do
        "http" -> :http
        "https" -> :https
        _other -> raise ArgumentError, "unexpected HTTP scheme: #{inspect(scheme)}"
      end

    initial_pool_state = %{
      template: {scheme, host, port}
    }

    NimblePool.start_link(
      worker: {__MODULE__, initial_pool_state},
      pool_size: pool_size,
      worker_idle_timeout: worker_idle_timeout,
      lazy: true,
      name: name
    )
  end

  @doc """
  Returns a child spec to allow Ch pool to be started under a supervisor.

  ## Options

  The options are exactly the same as for `start_link/1`.
  """
  @spec child_spec([start_option]) :: Supervisor.child_spec()
  def child_spec(options) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [options]}}
  end

  @doc """
  Stops the given `pool`.

  The pool exits with the given `reason`. The pool has `timeout` milliseconds to stop
  before it's unilaterally killed by the runtime.
  """
  @spec stop(NimblePool.pool(), reason :: term, timeout) :: :ok
  def stop(pool, reason \\ :normal, timeout \\ :infinity) do
    NimblePool.stop(pool, reason, timeout)
  end

  @doc """
  Executes a query on the given pool.

  Returns `{:ok, query_result}` on success or `{:error, query_error}` on failure.
  """
  @spec query(NimblePool.pool() | Connection.t(), query_statement, query_params, [query_option]) ::
          {:ok, query_result} | {:error, query_error}
  def query(client, statement, params \\ %{}, options \\ [])

  def query(%Connection{} = client, statement, params, options) do
    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)
    {settings, options} = Keyword.pop(options, :settings, [])
    {query, options} = Keyword.pop(options, :query, [])
    {headers, options} = Keyword.pop(options, :headers, [])

    deadline = Ch.HTTP.to_deadline(timeout)
    path = Ch.HTTP.path(params, query_options(settings, query))
    headers = merge_headers(@query_headers, headers)

    client.conn
    |> request_raw("POST", path, headers, statement, deadline)
    |> decode_query_result(options)
  end

  def query(pool, statement, params, options) do
    {timeout, options} = Keyword.pop(options, :timeout, @query_timeout)
    {settings, options} = Keyword.pop(options, :settings, [])
    {query, options} = Keyword.pop(options, :query, [])
    {headers, options} = Keyword.pop(options, :headers, [])

    deadline = Ch.HTTP.to_deadline(timeout)
    path = Ch.HTTP.path(params, query_options(settings, query))
    headers = merge_headers(@query_headers, headers)

    result =
      run(pool, timeout, fn conn ->
        request_raw(conn, "POST", path, headers, statement, deadline)
      end)

    decode_query_result(result, options)
  end

  def query!(pool, statement, params \\ %{}, options \\ []) do
    case query(pool, statement, params, options) do
      {:ok, result} -> result
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Checks out one connection for the duration of `fun`.
  """
  @spec checkout(NimblePool.pool(), (Connection.t() -> result), [query_option]) :: result
        when result: var
  def checkout(pool, fun, options \\ []) when is_function(fun, 1) do
    {timeout, _options} = Keyword.pop(options, :timeout, @query_timeout)

    run(pool, timeout, fun)
  end

  defp run(pool, timeout, fun) do
    deadline = Ch.HTTP.to_deadline(timeout)

    NimblePool.checkout!(
      pool,
      :request,
      fn {pid, _ref}, conn_or_template ->
        with {:ok, conn, pool_pid} <- connect(conn_or_template, pid, deadline) do
          result = fun.(%Connection{conn: conn, pool: pool_pid})
          {result, checkin(conn, pool_pid)}
        else
          {:error, reason} = error -> {error, {:remove, reason}}
        end
      end,
      timeout
    )
  end

  defp request_raw(%Connection{} = client, method, path, headers, body, deadline) do
    request_raw(client.conn, method, path, headers, body, deadline)
  end

  defp request_raw(conn, method, path, headers, body, deadline) do
    with {:ok, _conn, status, headers, data} <-
           request(conn, method, path, headers, body, deadline) do
      {:ok, status, headers, data}
    end
  end

  defp decode_query_result({:ok, status, headers, data}, options) do
    data = data |> maybe_decompress(headers) |> IO.iodata_to_binary()
    decode_query_response(status, headers, data, options)
  end

  defp decode_query_result({:error, _reason} = error, _options), do: error
  defp decode_query_result(:ok, _options), do: :ok

  @impl NimblePool
  def init_pool(config) do
    {:ok, config}
  end

  @impl NimblePool
  def init_worker(config) do
    {:ok, :template, config}
  end

  @impl NimblePool
  def handle_checkout(:request, _from, :template = template, config) do
    {:ok, {:template, config.template, self()}, template, config}
  end

  def handle_checkout(:request, _from, %Mint.HTTP1{} = conn, config) do
    {:ok, {:conn, conn, self()}, conn, config}
  end

  @impl NimblePool
  def handle_checkin({:ok, conn}, _from, _conn, config) do
    {:ok, conn, config}
  end

  def handle_checkin({:remove, reason}, _from, _conn, config) do
    {:remove, reason, config}
  end

  @impl NimblePool
  def handle_ping(_conn, _config) do
    {:remove, :worker_idle_timeout}
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, config) do
    with %Mint.HTTP1{} <- conn, do: Mint.HTTP1.close(conn)
    {:ok, config}
  end

  defp connect({:template, {scheme, host, port}, pool_pid}, owner, deadline) do
    timeout = Ch.HTTP.to_timeout(deadline)

    case Mint.HTTP1.connect(scheme, host, port, mode: :passive, timeout: timeout) do
      {:ok, conn} ->
        case Mint.HTTP1.controlling_process(conn, owner) do
          {:ok, conn} ->
            {:ok, conn, pool_pid}

          {:error, _reason} = error ->
            Mint.HTTP1.close(conn)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp connect({:conn, conn, pool_pid}, owner, _deadline) do
    case Mint.HTTP1.controlling_process(conn, owner) do
      {:ok, conn} -> {:ok, conn, pool_pid}
      {:error, reason} -> {:error, reason}
    end
  end

  defp request(conn, method, path, headers, body, deadline) do
    result =
      with {:ok, conn, _ref} <- Mint.HTTP1.request(conn, method, path, headers, body) do
        recv_all(conn, nil, [], [], deadline)
      end

    with {:error, conn, reason} <- result do
      Mint.HTTP1.close(conn)
      {:error, reason}
    end
  end

  defp recv_all(conn, status, headers, data, deadline) do
    timeout = Ch.HTTP.to_timeout(deadline)

    case Mint.HTTP1.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, status, headers, data) do
          {:ok, status, headers, data} -> {:ok, conn, status, headers, data}
          {:more, status, headers, data} -> recv_all(conn, status, headers, data, deadline)
          {:error, reason} -> {:error, conn, reason}
        end

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}
    end
  end

  defp handle_responses([{:status, _ref, status} | rest], _prev_status = nil, headers, data) do
    handle_responses(rest, status, headers, data)
  end

  defp handle_responses([{:headers, _ref, new_headers} | rest], status, prev_headers, data) do
    handle_responses(rest, status, prev_headers ++ new_headers, data)
  end

  defp handle_responses([{:data, _ref, new_data} | rest], status, headers, prev_data) do
    handle_responses(rest, status, headers, [prev_data | new_data])
  end

  defp handle_responses([{:done, _ref}], status, headers, data) do
    {:ok, status, headers, data}
  end

  defp handle_responses([{:error, _ref, reason} | _rest], _status, _headers, _data) do
    {:error, reason}
  end

  defp handle_responses([], status, headers, data) do
    {:more, status, headers, data}
  end

  defp checkin(conn, pool_pid) do
    cond do
      !Mint.HTTP1.open?(conn) ->
        {:remove, Mint.TransportError.exception(reason: :closed)}

      true ->
        case Mint.HTTP1.controlling_process(conn, pool_pid) do
          {:ok, conn} -> {:ok, conn}
          {:error, reason} -> {:remove, reason}
        end
    end
  end

  defp query_options(settings, query) do
    Enum.to_list(settings) ++ Enum.to_list(query)
  end

  defp merge_headers(defaults, overrides) do
    Enum.reduce(overrides, defaults, fn {key, value}, acc ->
      acc
      |> Enum.reject(fn {existing, _} -> String.downcase(existing) == String.downcase(key) end)
      |> List.insert_at(-1, {key, value})
    end)
  end

  defp maybe_decompress(data, headers) do
    case List.keyfind(headers, "content-encoding", 0) do
      {_, "gzip"} -> :zlib.gunzip(data)
      {_, "zstd"} -> :zstd.decompress(data)
      {_, other} -> raise "unsupported content encoding: #{inspect(other)}"
      nil -> data
    end
  end

  defp decode_query_response(200, _headers, _no_body = "", _options) do
    :ok
  end

  defp decode_query_response(200, headers, body, options) do
    if Keyword.get(options, :decode, true) == false do
      {:ok, body}
    else
      case List.keyfind(headers, "x-clickhouse-format", 0) do
        {_, "RowBinaryWithNamesAndTypes"} ->
          [name | rows] = Ch.RowBinary.decode_names_and_rows(body)
          {:ok, %{names: name, rows: rows}}

        {_, _format} ->
          {:ok, body}

        nil ->
          {:ok, body}
      end
    end
  end

  defp decode_query_response(_status, headers, body, _options) do
    code =
      case List.keyfind(headers, "x-clickhouse-exception-code", 0) ||
             List.keyfind(headers, "x-clickhouse-error-code", 0) do
        {_, code} -> String.to_integer(code)
        nil -> nil
      end

    {:error, %Ch.Error{code: code, message: body}}
  end
end
