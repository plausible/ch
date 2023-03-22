defmodule Ch.Connection do
  @moduledoc false
  use DBConnection
  require Logger
  alias Ch.{Error, RowBinary, Query, Result}
  alias Mint.HTTP1, as: HTTP

  @typep conn :: HTTP.t()

  @impl true
  @spec connect(Keyword.t()) :: {:ok, conn} | {:error, Error.t() | Mint.Types.error()}
  def connect(opts) do
    scheme = String.to_existing_atom(opts[:scheme] || "http")
    address = opts[:hostname] || "localhost"
    port = opts[:port] || 8123
    mint_opts = [mode: :passive] ++ Keyword.take(opts, [:hostname, :transport_opts])

    with {:ok, conn} <- HTTP.connect(scheme, address, port, mint_opts) do
      conn =
        conn
        |> HTTP.put_private(:timeout, opts[:timeout] || :timer.seconds(15))
        |> maybe_put_private(:database, opts[:database])
        |> maybe_put_private(:username, opts[:username])
        |> maybe_put_private(:password, opts[:password])
        |> maybe_put_private(:settings, opts[:settings])

      handshake = "select 1"

      case handle_execute(Query.build(handshake), [], [], conn) do
        {:ok, _query, %Result{rows: [[1]]}, conn} ->
          {:ok, conn}

        {:ok, _query, result, _conn} ->
          {:error, Error.exception("unexpected result for '#{handshake}': " <> inspect(result))}

        {:error, reason, _conn} ->
          {:error, reason}

        {:disconnect, reason, _conn} ->
          {:error, reason}
      end
    end
  end

  @impl true
  @spec ping(conn) :: {:ok, conn} | {:disconnect, Mint.Types.error() | Error.t(), conn}
  def ping(conn) do
    case request(conn, "GET", "/ping", _headers = [], _body = "", _opts = []) do
      {:ok, conn, _response} -> {:ok, conn}
      {:error, error, conn} -> {:disconnect, error, conn}
      {:disconnect, _error, _conn} = disconnect -> disconnect
    end
  end

  @impl true
  @spec checkout(conn) :: {:ok, conn}
  def checkout(conn), do: {:ok, conn}

  # "supporting" transactions for Repo.checkout

  @impl true
  def handle_begin(_opts, conn), do: {:ok, %{}, conn}
  @impl true
  def handle_commit(_opts, conn), do: {:ok, %{}, conn}
  @impl true
  def handle_rollback(_opts, conn), do: {:ok, %{}, conn}
  @impl true
  def handle_status(_opts, conn), do: {:idle, conn}

  @impl true
  def handle_prepare(_query, _opts, conn) do
    {:error, Error.exception("prepared statements are not supported"), conn}
  end

  @impl true
  def handle_close(_query, _opts, conn) do
    {:error, Error.exception("prepared statements are not supported"), conn}
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
  def handle_execute(%Query{command: :insert} = query, %s{} = stream, opts, conn)
      when s in [Stream, IO.Stream, File.Stream] do
    path = path(settings(conn, opts))
    headers = headers(conn, opts)
    stream = Stream.concat([[query.statement, ?\n]], stream)

    with {:ok, conn, responses} <- request_chunked(conn, "POST", path, headers, stream, opts) do
      {:ok, query, insert_result(responses), conn}
    end
  end

  def handle_execute(%Query{command: :insert} = query, {:raw, data}, opts, conn)
      when is_list(data) or is_binary(data) do
    path = path(settings(conn, opts))
    headers = headers(conn, opts)
    body = [query.statement, ?\n | data]

    with {:ok, conn, responses} <- request(conn, "POST", path, headers, body, opts) do
      {:ok, query, insert_result(responses), conn}
    end
  end

  def handle_execute(%Query{command: :insert} = query, params, opts, conn) do
    path = path(settings(conn, opts) ++ params(params))
    headers = headers(conn, opts)

    with {:ok, conn, responses} <- request(conn, "POST", path, headers, query.statement, opts) do
      {:ok, query, insert_result(responses), conn}
    end
  end

  def handle_execute(query, params, opts, conn) do
    %Query{command: command, statement: statement} = query

    types = Keyword.get(opts, :types)
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format
    path = path(settings(conn, opts) ++ params(params))
    headers = [{"x-clickhouse-format", format} | headers(conn, opts)]

    with {:ok, conn, responses} <- request(conn, "POST", path, headers, statement, opts) do
      {:ok, query, result(command, responses, types), conn}
    end
  end

  @impl true
  def disconnect(_error, conn) do
    {:ok = ok, _conn} = HTTP.close(conn)
    ok
  end

  defp insert_result(responses) do
    [_status, headers | _data] = responses
    meta = meta(headers)

    num_rows =
      if written_rows = get_in(meta, ["summary", "written_rows"]) do
        String.to_integer(written_rows)
      end

    %Result{num_rows: num_rows, rows: nil, meta: meta, command: :insert}
  end

  defp result(command, responses, types) do
    [_status, headers | data] = responses
    meta = meta(headers)

    rows =
      case Map.get(meta, "format") do
        "RowBinary" ->
          data |> IO.iodata_to_binary() |> RowBinary.decode_rows(types)

        "RowBinaryWithNamesAndTypes" ->
          data |> IO.iodata_to_binary() |> RowBinary.decode_rows()

        _other ->
          data
      end

    %Result{num_rows: length(rows), rows: rows, meta: meta, command: command}
  end

  @typep response :: Mint.Types.status() | Mint.Types.headers() | binary

  @spec request(conn, binary, binary, Mint.Types.headers(), iodata, Keyword.t()) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  defp request(conn, method, path, headers, body, opts) do
    with {:ok, conn, ref} <- send_request(conn, method, path, headers, body) do
      receive_response(conn, ref, timeout(conn, opts))
    end
  end

  @spec request_chunked(conn, binary, binary, Mint.Types.headers(), Enumerable.t(), Keyword.t()) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  def request_chunked(conn, method, path, headers, stream, opts) do
    with {:ok, conn, ref} <- send_request(conn, method, path, headers, :stream),
         {:ok, conn} <- stream_body(conn, ref, stream),
         do: receive_response(conn, ref, timeout(conn, opts))
  end

  @spec stream_body(conn, Mint.Types.request_ref(), Enumerable.t()) ::
          {:ok, conn} | {:disconnect, Mint.Types.error(), conn}
  defp stream_body(conn, ref, stream) do
    result =
      stream
      |> Stream.concat([:eof])
      |> Enum.reduce_while({:ok, conn}, fn
        chunk, {:ok, conn} -> {:cont, HTTP.stream_request_body(conn, ref, chunk)}
        _chunk, {:error, _conn, _reason} = error -> {:halt, error}
      end)

    case result do
      {:ok, _conn} = ok -> ok
      {:error, conn, reason} -> {:disconnect, reason, conn}
    end
  end

  # stacktrace is a bit cleaner with this function inlined
  @compile inline: [send_request: 5]
  defp send_request(conn, method, path, headers, body) do
    case HTTP.request(conn, method, path, headers, body) do
      {:ok, _conn, _ref} = ok -> ok
      {:error, conn, reason} -> {:disconnect, reason, conn}
    end
  end

  @spec receive_response(conn, Mint.Types.request_ref(), timeout) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  defp receive_response(conn, ref, timeout) do
    with {:ok, conn, responses} <- recv(conn, ref, [], timeout) do
      case responses do
        [200, headers | _rest] ->
          conn = ensure_same_server(conn, headers)
          {:ok, conn, responses}

        [_status, headers | data] ->
          message = IO.iodata_to_binary(data)

          code =
            if code = get_header(headers, "x-clickhouse-exception-code") do
              String.to_integer(code)
            end

          {:error, Error.exception(code, message), conn}
      end
    end
  end

  @spec recv(conn, Mint.Types.request_ref(), [response], timeout()) ::
          {:ok, conn, [response]} | {:disconnect, Mint.Types.error(), conn}
  defp recv(conn, ref, acc, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, ref, acc) do
          {:ok, responses} -> {:ok, conn, responses}
          {:more, acc} -> recv(conn, ref, acc, timeout)
        end

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, conn}
    end
  end

  defp maybe_put_private(conn, _k, nil), do: conn
  defp maybe_put_private(conn, k, v), do: HTTP.put_private(conn, k, v)

  defp get_opts_or_private(conn, opts, key) do
    Keyword.get(opts, key) || HTTP.get_private(conn, key)
  end

  defp settings(conn, opts) do
    default_settings = HTTP.get_private(conn, :settings, [])
    opts_settings = Keyword.get(opts, :settings, [])
    Keyword.merge(default_settings, opts_settings)
  end

  defp headers(conn, opts) do
    []
    |> maybe_put_header("x-clickhouse-user", get_opts_or_private(conn, opts, :username))
    |> maybe_put_header("x-clickhouse-key", get_opts_or_private(conn, opts, :password))
    |> maybe_put_header("x-clickhouse-database", get_opts_or_private(conn, opts, :database))
  end

  defp maybe_put_header(headers, _k, nil), do: headers
  defp maybe_put_header(headers, k, v), do: [{k, v} | headers]

  defp timeout(conn) do
    HTTP.get_private(conn, :timeout)
  end

  defp timeout(conn, opts) do
    Keyword.get(opts, :timeout) || timeout(conn)
  end

  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil = not_found -> not_found
    end
  end

  defp meta(headers) do
    Map.new(_meta(headers))
  end

  defp _meta([{"x-clickhouse-summary" = k, summary} | headers]) do
    "x-clickhouse-" <> k = k
    [{k, Jason.decode!(summary)} | _meta(headers)]
  end

  defp _meta([{"x-clickhouse-" <> k, v} | headers]), do: [{k, v} | _meta(headers)]
  defp _meta([{_k, _v} | headers]), do: _meta(headers)
  defp _meta([]), do: []

  @server_display_name_key :server_display_name

  @spec ensure_same_server(conn, Mint.Types.headers()) :: conn
  defp ensure_same_server(conn, headers) do
    expected_name = HTTP.get_private(conn, @server_display_name_key)
    actual_name = get_header(headers, "x-clickhouse-server-display-name")

    cond do
      expected_name && actual_name ->
        unless actual_name == expected_name do
          Logger.warn(
            "Server mismatch detected. Expected #{inspect(expected_name)} but got #{inspect(actual_name)}!" <>
              " Connection pooling might be unstable."
          )
        end

        conn

      actual_name ->
        HTTP.put_private(conn, @server_display_name_key, actual_name)

      true ->
        conn
    end
  end

  defp handle_responses([{:done, ref}], ref, acc) do
    {:ok, :lists.reverse(acc)}
  end

  defp handle_responses([{tag, ref, data} | rest], ref, acc)
       when tag in [:data, :status, :headers] do
    handle_responses(rest, ref, [data | acc])
  end

  defp handle_responses([], _ref, acc), do: {:more, acc}

  defp path(kv) do
    "/?" <> URI.encode_query(kv)
  end

  defp params(params) when is_map(params) do
    Enum.map(params, fn {k, v} -> {"param_#{k}", encode_param(v)} end)
  end

  defp params(params) when is_list(params) do
    params
    |> Enum.with_index()
    |> Enum.map(fn {v, idx} -> {"param_$#{idx}", encode_param(v)} end)
  end

  defp encode_param(n) when is_integer(n), do: Integer.to_string(n)
  defp encode_param(f) when is_float(f), do: Float.to_string(f)
  defp encode_param(b) when is_binary(b), do: b
  defp encode_param(b) when is_boolean(b), do: b
  defp encode_param(%Decimal{} = d), do: Decimal.to_string(d, :normal)

  defp encode_param(%Date{} = date), do: date

  defp encode_param(%NaiveDateTime{} = naive) do
    NaiveDateTime.to_iso8601(naive)
  end

  defp encode_param(%DateTime{} = dt) do
    dt |> DateTime.to_naive() |> NaiveDateTime.to_iso8601()
  end

  defp encode_param(a) when is_list(a) do
    IO.iodata_to_binary([?[, encode_array_params(a), ?]])
  end

  defp encode_array_params([last]), do: encode_array_param(last)

  defp encode_array_params([s | rest]) do
    [encode_array_param(s), ?, | encode_array_params(rest)]
  end

  defp encode_array_params([] = empty), do: empty

  defp encode_array_param(s) when is_binary(s) do
    [?', to_iodata(s, 0, s, []), ?']
  end

  defp encode_array_param(v) do
    encode_param(v)
  end

  @dialyzer {:no_improper_lists, to_iodata: 4, to_iodata: 5}

  @doc false
  # based on based on https://github.com/elixir-plug/plug/blob/main/lib/plug/html.ex#L41-L80
  def to_iodata(binary, skip, original, acc)

  escapes = [{?', "\\'"}, {?\\, "\\\\"}]

  for {match, insert} <- escapes do
    def to_iodata(<<unquote(match), rest::bits>>, skip, original, acc) do
      to_iodata(rest, skip + 1, original, [acc | unquote(insert)])
    end
  end

  def to_iodata(<<_char, rest::bits>>, skip, original, acc) do
    to_iodata(rest, skip, original, acc, 1)
  end

  def to_iodata(<<>>, _skip, _original, acc) do
    acc
  end

  for {match, insert} <- escapes do
    defp to_iodata(<<unquote(match), rest::bits>>, skip, original, acc, len) do
      part = binary_part(original, skip, len)
      to_iodata(rest, skip + len + 1, original, [acc, part | unquote(insert)])
    end
  end

  defp to_iodata(<<_char, rest::bits>>, skip, original, acc, len) do
    to_iodata(rest, skip, original, acc, len + 1)
  end

  defp to_iodata(<<>>, 0, original, _acc, _len) do
    original
  end

  defp to_iodata(<<>>, skip, original, acc, len) do
    [acc | binary_part(original, skip, len)]
  end
end
