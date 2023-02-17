defmodule Ch.Connection do
  @moduledoc false
  use DBConnection
  alias Ch.{Error, RowBinary}
  alias Mint.HTTP1, as: HTTP

  @impl true
  def connect(opts) do
    scheme = String.to_existing_atom(opts[:scheme] || "http")
    address = opts[:hostname] || "localhost"
    port = opts[:port] || 8123

    # TODO active: once, active: false, how to deal with checkout / controlling process?
    with {:ok, conn} <- HTTP.connect(scheme, address, port, mode: :passive) do
      conn =
        conn
        |> HTTP.put_private(:database, opts[:database] || "default")
        |> HTTP.put_private(:timeout, opts[:timeout] || :timer.seconds(15))
        |> maybe_put_private(:username, opts[:username])
        |> maybe_put_private(:password, opts[:password])

      {:ok, conn}
    end
  end

  @impl true
  def ping(conn) do
    with {:ok, conn, ref} <- request(conn, "GET", "/ping", _headers = [], _body = ""),
         {:ok, conn, _responses} <- receive_stream(conn, ref),
         do: {:ok, conn}
  end

  @impl true
  def checkout(conn) do
    {:ok, conn}
  end

  @impl true
  def handle_begin(_opts, conn) do
    {:ok, %{}, conn}
  end

  @impl true
  def handle_commit(_opts, conn) do
    {:ok, %{}, conn}
  end

  @impl true
  def handle_rollback(_opts, conn) do
    {:ok, %{}, conn}
  end

  @impl true
  def handle_status(_opts, conn) do
    {:idle, conn}
  end

  @impl true
  def handle_prepare(query, _opts, conn) do
    {:ok, query, conn}
  end

  @impl true
  def handle_execute(%Ch.Query{command: :insert} = query, rows, opts, conn) do
    %Ch.Query{statement: statement} = query

    path =
      case Keyword.get(opts, :settings, []) do
        [] ->
          "/"

        settings ->
          qs =
            settings
            |> Map.new(fn {k, v} -> {to_string(k), to_string(v)} end)
            |> URI.encode_query()

          "/?" <> qs
      end

    statement =
      if format = Keyword.get(opts, :format) do
        [statement, " FORMAT ", format, ?\n]
      else
        [statement, ?\n]
      end

    with {:ok, conn, ref} <- request(conn, "POST", path, headers(conn, opts), :stream),
         {:ok, conn} <- stream_body(conn, ref, statement, rows),
         {:ok, conn, responses} <- receive_stream(conn, ref) do
      [_status, headers | _data] = responses
      num_rows = get_summary(headers, "written_rows")
      {:ok, query, build_response(num_rows, _rows = []), conn}
    end
  end

  def handle_execute(query, params, opts, conn) do
    %Ch.Query{statement: statement} = query

    types = Keyword.get(opts, :types)
    settings = Keyword.get(opts, :settings, [])
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format

    params = build_params(params)
    params = Map.merge(params, Map.new(settings, fn {k, v} -> {to_string(k), to_string(v)} end))
    path = "/?" <> URI.encode_query(params)

    headers = [{"x-clickhouse-format", format} | headers(conn, opts)]

    with {:ok, conn, ref} <- request(conn, "POST", path, headers, statement),
         {:ok, conn, responses} <- receive_stream(conn, ref, opts) do
      [_status, headers | data] = responses

      response =
        case get_header(headers, "x-clickhouse-format") do
          "RowBinary" ->
            rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows(types)
            build_response(rows)

          "RowBinaryWithNamesAndTypes" ->
            rows = data |> IO.iodata_to_binary() |> RowBinary.decode_rows()
            build_response(rows)

          _other ->
            data
        end

      {:ok, query, response, conn}
    end
  end

  defp build_response(rows) do
    build_response(length(rows), rows)
  end

  defp build_response(num_rows, rows) do
    %{num_rows: num_rows, rows: rows}
  end

  @impl true
  def handle_close(_query, _opts, conn) do
    {:ok, _result = nil, conn}
  end

  @impl true
  def handle_declare(_query, _params, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, conn) do
    {:error, Error.exception("cursors are not supported"), conn}
  end

  @impl true
  def disconnect(_error, conn) do
    {:ok = ok, _conn} = HTTP.close(conn)
    ok
  end

  defp maybe_put_private(conn, _k, nil), do: conn
  defp maybe_put_private(conn, k, v), do: HTTP.put_private(conn, k, v)

  defp get_opts_or_private(conn, opts, key) do
    opts[key] || HTTP.get_private(conn, key)
  end

  defp headers(conn, opts) do
    []
    |> maybe_put_header("x-clickhouse-user", get_opts_or_private(conn, opts, :username))
    |> maybe_put_header("x-clickhouse-key", get_opts_or_private(conn, opts, :password))
    |> maybe_put_header("x-clickhouse-database", get_opts_or_private(conn, opts, :database))
  end

  defp maybe_put_header(headers, _k, nil), do: headers
  defp maybe_put_header(headers, k, v), do: [{k, v} | headers]

  # @compile inline: [request: 5]
  defp request(conn, method, path, headers, body) do
    case HTTP.request(conn, method, path, headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, _conn, _reason} = error -> disconnect(error)
    end
  end

  def stream_body(conn, ref, statement, data) do
    # TODO HTTP.stream_request_body(conn, ref, [statement, ?\n])?
    stream = Stream.concat([statement], data)

    # TODO bench vs manual
    reduced =
      Enum.reduce_while(stream, {:ok, conn}, fn
        chunk, {:ok, conn} -> {:cont, HTTP.stream_request_body(conn, ref, chunk)}
        _chunk, error -> {:halt, error}
      end)

    case reduced do
      {:ok, conn} ->
        case HTTP.stream_request_body(conn, ref, :eof) do
          {:ok, _conn} = ok -> ok
          {:error, _conn, _error} = error -> disconnect(error)
        end

      {:halt, {:error, _conn, _error} = error} ->
        disconnect(error)
    end
  end

  defp receive_stream(conn, ref, opts \\ []) do
    case receive_stream(conn, ref, [], opts) do
      {:ok, _conn, [200 | _rest]} = ok ->
        ok

      {:ok, conn, [_status, headers | data]} ->
        error = IO.iodata_to_binary(data)
        exception = Error.exception(error)

        code =
          if kv = List.keyfind(headers, "x-clickhouse-exception-code", 0) do
            String.to_integer(elem(kv, 1))
          end

        exception = %{exception | code: code}
        {:error, exception, conn}

      {:error, _conn, _error, _responses} = error ->
        disconnect(error)
    end
  end

  @typep response :: Mint.Types.status() | Mint.Types.headers() | binary

  @spec receive_stream(HTTP.t(), reference, [response], Keyword.t()) ::
          {:ok, HTTP.t(), [response]}
          | {:error, HTTP.t(), Mint.Types.error(), [response]}
  defp receive_stream(conn, ref, acc, opts) do
    timeout = opts[:timeout] || HTTP.get_private(conn, :timeout)

    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, ref, acc) do
          {:ok, responses} -> {:ok, conn, responses}
          {:more, acc} -> receive_stream(conn, ref, acc, opts)
        end

      {:error, _conn, _reason, responses} = error ->
        put_elem(error, 3, acc ++ responses)
    end
  end

  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil = not_found -> not_found
    end
  end

  # TODO telemetry?
  defp get_summary(headers) do
    if summary = get_header(headers, "x-clickhouse-summary") do
      Jason.decode!(summary)
    end
  end

  defp get_summary(headers, key) do
    if summary = get_summary(headers) do
      if value = Map.get(summary, key) do
        String.to_integer(value)
      end
    end
  end

  # TODO wrap errors in Ch.Error?
  @spec disconnect({:error, HTTP.t(), Mint.Types.error(), [response]}) ::
          {:disconnect, Mint.Types.error(), HTTP.t()}
  defp disconnect({:error, conn, error, _responses}) do
    {:disconnect, error, conn}
  end

  @spec disconnect({:error, HTTP.t(), Mint.Types.error()}) ::
          {:disconnect, Mint.Types.error(), HTTP.t()}
  defp disconnect({:error, conn, error}) do
    {:disconnect, error, conn}
  end

  # TODO handle rest
  defp handle_responses([{:done, ref}], ref, acc) do
    {:ok, :lists.reverse(acc)}
  end

  defp handle_responses([{tag, ref, data} | rest], ref, acc)
       when tag in [:data, :status, :headers] do
    handle_responses(rest, ref, [data | acc])
  end

  defp handle_responses([], _ref, acc), do: {:more, acc}

  # TODO support just one approach?
  defp build_params(params) when is_map(params) do
    params |> Map.new(fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp build_params([{_k, _v} | _] = params) do
    params |> Map.new(fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp build_params(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Map.new(fn {v, idx} -> {"param_$#{idx}", encode_param(v)} end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)
  defp encode_param(b) when is_binary(b), do: b

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  defp encode_param(%Date{} = date), do: date

  # TODO DateTime64 to include microseconds?
  defp encode_param(%NaiveDateTime{} = naive) do
    NaiveDateTime.diff(naive, @epoch_naive_datetime)
  end

  # TODO support non-GMT timezones?
  defp encode_param(%DateTime{time_zone: "Etc/UTC"} = dt) do
    DateTime.to_unix(dt)
  end

  defp encode_param(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_param(a), ?]])
  end

  # TODO [1, 2] => 1,2, (CH doesn't seem to mind trailing comma, but still...)
  defp encode_array_param([s | rest]) when is_binary(s) do
    # TODO faster escaping
    # TODO \\\\
    [?', String.replace(s, "'", "\\'"), "'," | encode_array_param(rest)]
  end

  defp encode_array_param([el | rest]) do
    [encode_param(el), "," | encode_array_param(rest)]
  end

  defp encode_array_param([] = done), do: done
end
