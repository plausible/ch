defmodule Ch do
  @moduledoc """
  Minimal HTTP ClickHouse client.

  `Ch` starts a lazy pool of HTTP/1 connections to ClickHouse. The pool opens
  connections on demand and reuses them while they remain healthy.

  By default, queries request `RowBinaryWithNamesAndTypes` and return decoded
  rows:

      {:ok, pool} = Ch.start_link(url: "http://localhost:8123")

      {:ok, %Ch.Result{names: ["number"], rows: [[1], [2], [3]]}} =
        Ch.query(pool, "SELECT number FROM system.numbers LIMIT {limit:UInt8}", %{
          "limit" => 3
        })

  For large decoded responses, ask ClickHouse for compressed response bodies:

      Ch.query!(
        pool,
        "SELECT number FROM system.numbers LIMIT 1_000_000",
        %{},
        headers: [{"accept-encoding", "zstd"}]
      )

  `Ch` automatically decompresses successful responses that it decodes itself
  (`RowBinaryWithNamesAndTypes`) and error responses. Successful responses in
  other formats keep the raw response body in `Ch.Result.data`, including any
  `content-encoding`.
  """
  @behaviour NimblePool

  @dialyzer :no_improper_lists

  @query_timeout to_timeout(second: 30)
  @user_agent "ch/#{Ch.MixProject.version()}"

  @start_options_schema [
    name: [
      type: {:custom, __MODULE__, :validate_name, []},
      doc: """
      The name of the Ch pool instance, used to identify and interact with it. Supported values are atoms and via tuples.
      """
    ],
    pool_size: [
      type: :pos_integer,
      doc:
        "Maximum number of concurrent connections. Pool is lazy so it starts out without any connections and they are open on demand.",
      default: 20
    ],
    worker_idle_timeout: [
      type: :timeout,
      doc: """
      Time a connection can stay idle before the pool closes it.
      Should be lower than ClickHouse's [`keep_alive_timeout`](https://clickhouse.com/docs/operations/server-configuration-parameters/settings#keep_alive_timeout)
      to avoid sending a request over a connection that would be closed by ClickHouse soon-ish.
      """,
      default: to_timeout(second: 5)
    ],
    url: [
      type: :string,
      doc: "The ClickHouse endpoint URL.",
      default: "http://localhost:8123"
    ]
  ]

  @doc false
  def validate_name(name) when is_atom(name), do: {:ok, name}
  def validate_name({:via, module, _term} = via) when is_atom(module), do: {:ok, via}

  def validate_name(name) do
    {:error,
     "expected :name to be an atom or a {:via, module, term} tuple, got: #{inspect(name)}"}
  end

  @typedoc """
  The query payload.

  This can be a standard SQL string or SQL appended with data (`[sql, ?\n, rowbinary]`).
  If providing compressed payloads, don't forget to pass the appropriate `content-encoding` header.
  """
  @type query_statement :: iodata

  @typedoc """
  Named query parameters.

  Keys are parameter names without the ClickHouse `param_` prefix. Values are
  encoded in ClickHouse's escaped parameter format and passed in the URL query
  string.

      Ch.query(pool, "SELECT {name:String}", %{"name" => "Ada"})
  """
  @type query_params :: %{String.t() => term}

  @typedoc """
  Query execution options.

  * `:timeout` - Request timeout, defaults to 30 seconds.
  * `:settings` - An enumerable (usually a map or a keyword list) added to the URL query string.
  * `:headers` - Headers passed directly to Mint.
  * `:telemetry_metadata` - Extra metadata to include in telemetry events.
  """
  @type query_option ::
          {:timeout, timeout}
          | {:settings, Enumerable.t()}
          | {:headers, Mint.Types.headers()}
          | {:telemetry_metadata, map}

  @typedoc """
  The parsed query response.

  If the response format is `RowBinaryWithNamesAndTypes`, `Ch` returns decoded
  column names and rows in `Ch.Result`. Other successful formats keep the raw
  response body in `Ch.Result.data`.
  """
  @type query_result :: Ch.Result.t()

  @typedoc """
  A query execution error.

  Returns `Ch.Error` for ClickHouse errors or Mint errors for network/HTTP failures.
  """
  @type query_error :: Ch.Error.t() | Mint.Types.error()

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
      template: {:template, scheme, host, port}
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
  Executes a ClickHouse query.

  `statement` is usually a SQL string. It can also be iodata, which is useful
  for `INSERT ... FORMAT RowBinary` requests:

      rowbinary = Ch.RowBinary.encode_rows([[1, "Ada"]], ["UInt8", "String"])
      Ch.query!(pool, ["INSERT INTO users FORMAT RowBinary\n", rowbinary])

  `params` are named ClickHouse query parameters used by placeholders such as
  `{limit:UInt8}`.

  Options:

    * `:timeout` - request timeout, defaults to 30 seconds.
    * `:settings` - ClickHouse settings added to the URL query string.
    * `:headers` - HTTP headers sent with the request.
    * `:telemetry_metadata` - extra metadata included in query telemetry events.

  By default, `Ch` adds `x-clickhouse-format: RowBinaryWithNamesAndTypes`,
  decodes that response format, and returns `%Ch.Result{names: names, rows: rows}`.
  Passing a different `x-clickhouse-format` header disables automatic row
  decoding and keeps the response body in `%Ch.Result{data: data}`.

  If an error response is compressed with `gzip` or `zstd`, `Ch` decompresses it
  before returning `%Ch.Error{}`.
  """
  @spec query(NimblePool.pool(), query_statement, query_params, [query_option]) ::
          {:ok, query_result} | {:error, query_error}
  def query(pool, statement, params \\ %{}, options \\ []) do
    timeout = Keyword.get(options, :timeout, @query_timeout)
    settings = Keyword.get(options, :settings, [])
    telemetry_metadata = Keyword.get(options, :telemetry_metadata, %{})

    headers =
      options
      |> Keyword.get(:headers, [])
      |> put_new_header("user-agent", @user_agent)
      |> put_new_header("x-clickhouse-format", "RowBinaryWithNamesAndTypes")

    deadline = Ch.HTTP.to_deadline(timeout)
    path = Ch.HTTP.path(params, settings)
    query_started = System.monotonic_time()
    checkout_started = System.monotonic_time()

    metadata = %{
      pool: pool,
      telemetry_metadata: telemetry_metadata,
      settings: settings,
      format: get_header(headers, "x-clickhouse-format")
    }

    :telemetry.execute([:ch, :query, :start], %{system_time: System.system_time()}, metadata)

    try do
      result =
        NimblePool.checkout!(
          pool,
          :request,
          fn {pid, _ref}, conn_or_template ->
            meter = [] |> past_event(:checkout, checkout_started) |> event(:query)

            with {:ok, conn} <- connect(conn_or_template, pid, deadline, metadata),
                 {:ok, conn, status, headers, data} <-
                   request(conn, "POST", path, headers, statement, deadline) do
              {{:ok, status, headers, data, meter}, checkin(conn)}
            else
              {:error, reason} ->
                {{:error, reason, meter}, {:remove, reason}}
            end
          end,
          timeout
        )

      case result do
        {:ok, status, headers, data, meter} ->
          meter = event(meter, :decode)

          try do
            case decode_query_response(status, headers, data) do
              {:ok, result} ->
                :telemetry.execute(
                  [:ch, :query, :stop],
                  telemetry_measurements(meter, query_started, result),
                  telemetry_metadata(metadata, status, headers, result)
                )

                {:ok, result}

              {:error, reason} ->
                :telemetry.execute(
                  [:ch, :query, :error],
                  telemetry_measurements(meter, query_started, nil),
                  telemetry_error_metadata(metadata, status, headers, reason, nil)
                )

                {:error, reason}
            end
          catch
            kind, reason ->
              stacktrace = __STACKTRACE__

              :telemetry.execute(
                [:ch, :query, :error],
                telemetry_measurements(meter, query_started, nil),
                telemetry_error_metadata(metadata, status, headers, reason, {kind, stacktrace})
              )

              :erlang.raise(kind, reason, stacktrace)
          end

        {:error, reason, meter} ->
          :telemetry.execute(
            [:ch, :query, :error],
            telemetry_measurements(meter, query_started, nil),
            telemetry_error_metadata(metadata, nil, [], reason, nil)
          )

          {:error, reason}
      end
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__

        :telemetry.execute(
          [:ch, :query, :error],
          telemetry_measurements([], query_started, nil),
          Map.merge(metadata, %{
            kind: kind,
            reason: reason,
            stacktrace: stacktrace
          })
        )

        :erlang.raise(kind, reason, stacktrace)
    end
  end

  @doc """
  Executes a query on the given pool, raising on error.

  Returns the `query_result` directly. Raises an exception if the query fails.
  """
  @spec query!(NimblePool.pool(), query_statement, query_params, [query_option]) :: query_result
  def query!(pool, statement, params \\ %{}, options \\ []) do
    case query(pool, statement, params, options) do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

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
    {:ok, config.template, template, config}
  end

  def handle_checkout(:request, _from, %Mint.HTTP1{} = conn, config) do
    {:ok, {:ok, conn}, conn, config}
  end

  def handle_checkout(:request, _from, {:conn, conn, checkin_time, conn_metadata}, config) do
    {:ok, {:ok, conn, checkin_time, conn_metadata}, conn, config}
  end

  @impl NimblePool
  def handle_checkin({:ok, conn}, _from, _prev, config) do
    {:ok, {:conn, conn, System.monotonic_time(), conn_metadata(config, %{})}, config}
  end

  def handle_checkin({:remove, reason}, _from, _prev, config) do
    {:remove, reason, config}
  end

  @impl NimblePool
  def handle_ping(_conn, _config) do
    {:remove, :worker_idle_timeout}
  end

  @impl NimblePool
  def terminate_worker(reason, conn_or_template, config) do
    case conn_or_template do
      :template -> :ok
      {:conn, conn, _checkin_time, _conn_metadata} -> Mint.HTTP1.close(conn)
      conn -> Mint.HTTP1.close(conn)
    end

    unless conn_or_template == :template do
      emit_conn_event([:ch, :conn, :drop], %{}, conn_metadata(config, %{reason: reason}))
    end

    {:ok, config}
  end

  defp connect({:template, scheme, host, port}, owner, deadline, query_metadata) do
    started = System.monotonic_time()
    timeout = Ch.HTTP.to_timeout(deadline)
    metadata = conn_metadata(scheme, host, port, query_metadata)

    emit_conn_event([:ch, :conn, :start], %{system_time: System.system_time()}, metadata)

    case Mint.HTTP1.connect(scheme, host, port, mode: :passive, timeout: timeout) do
      {:ok, conn} ->
        case Mint.HTTP1.controlling_process(conn, owner) do
          {:ok, _conn} = ok ->
            emit_conn_event([:ch, :conn, :stop], %{duration: duration(started)}, metadata)
            ok

          {:error, _reason} = error ->
            Mint.HTTP1.close(conn)

            emit_conn_event(
              [:ch, :conn, :error],
              %{duration: duration(started)},
              conn_error_metadata(metadata, error)
            )

            error
        end

      {:error, _reason} = error ->
        emit_conn_event(
          [:ch, :conn, :error],
          %{duration: duration(started)},
          conn_error_metadata(metadata, error)
        )

        error
    end
  end

  defp connect({:ok, _conn, checkin_time, conn_metadata} = ok, _owner, _deadline, query_metadata) do
    measurements = %{idle_time: max(System.monotonic_time() - checkin_time, 0)}
    emit_conn_event([:ch, :conn, :reuse], measurements, Map.merge(conn_metadata, query_metadata))
    {:ok, elem(ok, 1)}
  end

  defp connect({:ok, _conn} = ok, _owner, _deadline, _query_metadata), do: ok

  defp request(conn, method, path, headers, body, deadline) do
    result =
      with {:ok, conn, _ref} <- Mint.HTTP1.request(conn, method, path, headers, body) do
        recv_all(conn, nil, [], nil, deadline)
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
    next_data =
      case prev_data do
        nil -> new_data
        _ -> [prev_data | new_data]
      end

    handle_responses(rest, status, headers, next_data)
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

  defp checkin(conn) do
    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:remove, Mint.TransportError.exception(reason: :closed)}
    end
  end

  defp maybe_decompress(data, headers) do
    case get_header(headers, "content-encoding") do
      "zstd" when data != nil -> data |> IO.iodata_to_binary() |> :zstd.decompress()
      "gzip" when data != nil -> data |> IO.iodata_to_binary() |> :zlib.gunzip()
      nil -> data
      _ when data == nil -> data
      other -> raise "unsupported content encoding: #{inspect(other)}"
    end
  end

  defp decode_query_response(200, headers, body) do
    format = get_header(headers, "x-clickhouse-format")

    if format == "RowBinaryWithNamesAndTypes" do
      case body |> maybe_decompress(headers) |> response_body_to_binary() do
        "" ->
          {:ok, %Ch.Result{headers: headers, data: body}}

        decoded_data ->
          [names | rows] = Ch.RowBinary.decode_names_and_rows(decoded_data)

          {:ok,
           %Ch.Result{
             names: names,
             rows: rows,
             headers: headers,
             data: body
           }}
      end
    else
      {:ok, %Ch.Result{headers: headers, data: body}}
    end
  end

  defp decode_query_response(_status, headers, body) do
    message =
      body
      |> maybe_decompress(headers)
      |> response_body_to_binary()

    code =
      headers
      |> get_clickhouse_error_code()
      |> maybe_string_to_integer()

    {:error, %Ch.Error{code: code, message: message}}
  end

  defp response_body_to_binary(nil), do: ""
  defp response_body_to_binary(body), do: IO.iodata_to_binary(body)

  defp event(events, name), do: [{name, System.monotonic_time()} | events]

  defp past_event(events, name, time), do: [{name, time} | events]

  defp telemetry_measurements(events, query_started, result) do
    stop = System.monotonic_time()

    measurements =
      events
      |> Enum.reduce({stop, %{duration: stop - query_started}}, fn
        {:decode, start}, {stop, measurements} ->
          {start, Map.put(measurements, :decode_time, stop - start)}

        {:query, start}, {stop, measurements} ->
          {start, Map.put(measurements, :query_time, stop - start)}

        {:checkout, start}, {stop, measurements} ->
          {start, Map.put(measurements, :queue_time, stop - start)}

        {:checkin, start}, {stop, measurements} ->
          {stop, Map.put(measurements, :idle_time, max(stop - start, 0))}
      end)
      |> elem(1)

    case result do
      %Ch.Result{rows: rows, names: names, data: data} ->
        measurements
        |> maybe_put_measurement(:num_rows, rows && length(rows))
        |> maybe_put_measurement(:num_columns, names && length(names))
        |> maybe_put_measurement(:response_body_bytes, data && IO.iodata_length(data))

      _ ->
        measurements
    end
  end

  defp maybe_put_measurement(measurements, _name, nil), do: measurements
  defp maybe_put_measurement(measurements, name, value), do: Map.put(measurements, name, value)

  defp telemetry_metadata(metadata, status, headers, result) do
    Map.merge(metadata, %{
      status: status,
      response_headers: headers,
      result: result
    })
  end

  defp telemetry_error_metadata(metadata, status, headers, reason, stacktrace) do
    metadata
    |> Map.merge(%{
      status: status,
      response_headers: headers,
      kind: error_kind(stacktrace),
      reason: reason
    })
    |> maybe_put_metadata(:stacktrace, error_stacktrace(stacktrace))
    |> maybe_put_metadata(:clickhouse_error_code, clickhouse_error_code(reason))
  end

  defp error_kind({kind, _stacktrace}), do: kind
  defp error_kind(_stacktrace), do: :error

  defp error_stacktrace({_kind, stacktrace}), do: stacktrace
  defp error_stacktrace(stacktrace), do: stacktrace

  defp maybe_put_metadata(metadata, _key, nil), do: metadata
  defp maybe_put_metadata(metadata, key, value), do: Map.put(metadata, key, value)

  defp clickhouse_error_code(%Ch.Error{code: code}), do: code
  defp clickhouse_error_code(_reason), do: nil

  defp emit_conn_event(event, measurements, metadata) do
    :telemetry.execute(event, measurements, metadata)
  end

  defp conn_metadata(%{template: {:template, scheme, host, port}}, metadata) do
    conn_metadata(scheme, host, port, metadata)
  end

  defp conn_metadata(scheme, host, port, metadata) do
    Map.merge(metadata, %{
      scheme: scheme,
      host: host,
      port: port
    })
  end

  defp conn_error_metadata(metadata, {:error, reason}) do
    Map.merge(metadata, %{kind: :error, reason: reason})
  end

  defp duration(started), do: System.monotonic_time() - started

  @compile inline: [get_header: 2]
  defp get_header(headers, name) do
    with {_, value} <- List.keyfind(headers, name, 0, nil), do: value
  end

  defp get_clickhouse_error_code(headers) do
    get_header(headers, "x-clickhouse-error-code") ||
      get_header(headers, "x-clickhouse-exception-code")
  end

  defp maybe_string_to_integer(nil), do: nil
  defp maybe_string_to_integer(value), do: String.to_integer(value)

  @compile inline: [put_new_header: 3]
  defp put_new_header(headers, name, value) do
    if List.keymember?(headers, name, 0) do
      headers
    else
      [{name, value} | headers]
    end
  end

  if Code.ensure_loaded?(Ecto.ParameterizedType) do
    @behaviour Ecto.ParameterizedType

    @impl Ecto.ParameterizedType
    def type(:string), do: :string
    def type(:boolean), do: :boolean
    def type(:uuid), do: Ecto.UUID
    def type(:date), do: :date
    def type(:date32), do: :date
    def type(:time), do: :time
    def type({:time64, _p}), do: :time
    def type(:datetime), do: :naive_datetime
    def type({:datetime, _tz}), do: :utc_datetime
    def type({:datetime64, _p}), do: :naive_datetime_usec
    def type({:datetime64, _p, _tz}), do: :utc_datetime_usec
    def type({:fixed_string, _s}), do: :string
    def type(:json), do: :map
    def type(:dynamic), do: :any

    for size <- [8, 16, 32, 64, 128, 256] do
      def type(unquote(:"i#{size}")), do: :integer
      def type(unquote(:"u#{size}")), do: :integer
    end

    for size <- [32, 64] do
      def type(unquote(:"f#{size}")), do: :float
    end

    def type({:decimal, _p, _s}), do: :decimal

    for size <- [32, 64, 128, 256] do
      def type({unquote(:"decimal#{size}"), _s}) do
        :decimal
      end
    end

    def type({:array, type}), do: {:array, type(type)}
    def type({:nullable, type}), do: type(type)
    def type({:low_cardinality, type}), do: type(type)
    def type({:simple_aggregate_function, _name, type}), do: type(type)
    def type(:ring), do: {:array, type(:point)}
    def type(:polygon), do: {:array, type(:ring)}
    def type(:multipolygon), do: {:array, type(:polygon)}
    def type({enum, _mappings}) when enum in [:enum8, :enum16], do: :any
    def type(:ipv4), do: :any
    def type(:ipv6), do: :any
    def type(:point), do: :any
    def type({:tuple, _types}), do: :any
    def type({:map, _key_type, _value_type}), do: :map
    def type({:variant, _types}), do: :any

    @impl Ecto.ParameterizedType
    def init(opts) do
      clickhouse_type =
        opts[:raw] || opts[:type] ||
          raise ArgumentError, "keys :raw or :type not found in: #{inspect(opts)}"

      Ch.Types.decode(clickhouse_type)
    end

    @impl Ecto.ParameterizedType
    def load(value, _loader, _params), do: {:ok, value}

    @impl Ecto.ParameterizedType
    def dump(value, _dumper, _params), do: {:ok, value}

    @impl Ecto.ParameterizedType
    def cast(value, :string = type), do: Ecto.Type.cast(type, value)
    def cast(value, :boolean = type), do: Ecto.Type.cast(type, value)
    def cast(value, :uuid), do: Ecto.Type.cast(Ecto.UUID, value)
    def cast(value, :date = type), do: Ecto.Type.cast(type, value)
    def cast(value, :date32), do: Ecto.Type.cast(:date, value)
    def cast(value, :time = type), do: Ecto.Type.cast(type, value)
    def cast(value, {:time64, _p}), do: Ecto.Type.cast(:time, value)
    def cast(value, :datetime), do: Ecto.Type.cast(:naive_datetime, value)
    def cast(value, {:datetime, _tz}), do: Ecto.Type.cast(:utc_datetime, value)
    def cast(value, {:datetime64, _p}), do: Ecto.Type.cast(:naive_datetime_usec, value)
    def cast(value, {:datetime64, _p, _tz}), do: Ecto.Type.cast(:utc_datetime_usec, value)
    def cast(value, {:fixed_string, _s}), do: Ecto.Type.cast(:string, value)
    def cast(value, :json), do: Ecto.Type.cast(:map, value)
    def cast(value, :dynamic), do: {:ok, value}

    for size <- [8, 16, 32, 64, 128, 256] do
      def cast(value, unquote(:"i#{size}")), do: Ecto.Type.cast(:integer, value)
      def cast(value, unquote(:"u#{size}")), do: Ecto.Type.cast(:integer, value)
    end

    for size <- [32, 64] do
      def cast(value, unquote(:"f#{size}")), do: Ecto.Type.cast(:float, value)
    end

    def cast(value, {:decimal = type, _p, _s}), do: Ecto.Type.cast(type, value)

    for size <- [32, 64, 128, 256] do
      def cast(value, {unquote(:"decimal#{size}"), _s}) do
        Ecto.Type.cast(:decimal, value)
      end
    end

    def cast(value, {:array, type}), do: Ecto.Type.cast({:array, type(type)}, value)
    def cast(value, {:nullable, type}), do: cast(value, type)
    def cast(value, {:low_cardinality, type}), do: cast(value, type)
    def cast(value, {:simple_aggregate_function, _name, type}), do: cast(value, type)

    def cast(value, :ring), do: Ecto.Type.cast({:array, type(:point)}, value)
    def cast(value, :polygon), do: Ecto.Type.cast({:array, type(:ring)}, value)
    def cast(value, :multipolygon), do: Ecto.Type.cast({:array, type(:polygon)}, value)

    def cast(nil, _params), do: {:ok, nil}

    def cast(value, {enum, mappings}) when enum in [:enum8, :enum16] do
      result =
        case value do
          _ when is_integer(value) -> List.keyfind(mappings, value, 1, :error)
          _ when is_binary(value) -> List.keyfind(mappings, value, 0, :error)
          _ -> :error
        end

      case result do
        {_, _} -> {:ok, value}
        :error = e -> e
      end
    end

    def cast(value, :ipv4) do
      case value do
        {a, b, c, d} when is_number(a) and is_number(b) and is_number(c) and is_number(d) ->
          {:ok, value}

        _ when is_binary(value) ->
          with {:error = e, _reason} <- :inet.parse_ipv4_address(to_charlist(value)), do: e

        _ when is_list(value) ->
          with {:error = e, _reason} <- :inet.parse_ipv4_address(value), do: e

        _ ->
          :error
      end
    end

    def cast(value, :ipv6) do
      case value do
        {a, s, d, f, g, h, j, k}
        when is_number(a) and is_number(s) and is_number(d) and is_number(f) and
               is_number(g) and is_number(h) and is_number(j) and is_number(k) ->
          {:ok, value}

        _ when is_binary(value) ->
          with {:error = e, _reason} <- :inet.parse_ipv6_address(to_charlist(value)), do: e

        _ when is_list(value) ->
          with {:error = e, _reason} <- :inet.parse_ipv6_address(value), do: e

        _ ->
          :error
      end
    end

    def cast(value, :point) do
      case value do
        {x, y} when is_number(x) and is_number(y) -> {:ok, value}
        _ -> :error
      end
    end

    def cast(value, {:tuple, types}), do: cast_tuple(types, value)
    def cast(value, {:map, key_type, value_type}), do: cast_map(value, key_type, value_type)
    def cast(value, {:variant, types}), do: cast_variant(types, value)

    defp cast_tuple(types, values) when is_tuple(values) do
      cast_tuple(types, Tuple.to_list(values), [])
    end

    defp cast_tuple(types, values) when is_list(values) do
      cast_tuple(types, values, [])
    end

    defp cast_tuple(_types, _values), do: :error

    defp cast_tuple([type | types], [value | values], acc) do
      case cast(value, type) do
        {:ok, value} -> cast_tuple(types, values, [value | acc])
        :error = e -> e
      end
    end

    defp cast_tuple([], [], acc), do: {:ok, List.to_tuple(:lists.reverse(acc))}
    defp cast_tuple(_types, _values, _acc), do: :error

    defp cast_map(value, key_type, value_type) when is_map(value) do
      cast_map(Map.to_list(value), key_type, value_type)
    end

    defp cast_map(value, key_type, value_type) when is_list(value) do
      cast_map(value, key_type, value_type, [])
    end

    defp cast_map(_value, _key_type, _value_type), do: :error

    defp cast_map([{key, value} | kvs], key_type, value_type, acc) do
      with {:ok, key} <- cast(key, key_type),
           {:ok, value} <- cast(value, value_type) do
        cast_map(kvs, key_type, value_type, [{key, value} | acc])
      end
    end

    defp cast_map([], _key_type, _value_type, acc), do: {:ok, Map.new(acc)}
    defp cast_map(_kvs, _key_type, _value_type, _acc), do: :error

    defp cast_variant([type | types], value) do
      case cast(value, type) do
        {:ok, _value} = ok -> ok
        :error -> cast_variant(types, value)
      end
    end

    defp cast_variant([], _value), do: :error

    @impl Ecto.ParameterizedType
    def embed_as(_, _), do: :self

    @impl Ecto.ParameterizedType
    def equal?(a, b, _), do: a == b

    @impl Ecto.ParameterizedType
    def format(params) do
      "#Ch<#{Ch.Types.encode(params)}>"
    end
  end
end
