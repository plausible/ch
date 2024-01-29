defmodule Ch.Connection do
  @moduledoc false
  use DBConnection
  require Logger
  alias Ch.{Error, Query, Result}
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

      handshake = Query.build("select 1")
      params = DBConnection.Query.encode(handshake, _params = [], _opts = [])

      case handle_execute(handshake, params, _opts = [], conn) do
        {:ok, handshake, responses, conn} ->
          case DBConnection.Query.decode(handshake, responses, _opts = []) do
            %Result{rows: [[1]]} ->
              {:ok, conn}

            result ->
              {:ok, _conn} = HTTP.close(conn)
              reason = Error.exception("unexpected result for '#{handshake}': #{inspect(result)}")
              {:error, reason}
          end

        {:error, reason, conn} ->
          {:ok, _conn} = HTTP.close(conn)
          {:error, reason}

        {:disconnect, reason, conn} ->
          {:ok, _conn} = HTTP.close(conn)
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

  # we "support" these four tx callbacks for Repo.checkout
  # even though ClickHouse doesn't support txs

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
  def handle_declare(query, params, opts, conn) do
    {query_params, extra_headers, body} = params

    path = path(conn, query_params, opts)
    headers = headers(conn, extra_headers, opts)
    types = Keyword.get(opts, :types)

    with {:ok, conn, ref} <- send_request(conn, "POST", path, headers, body) do
      {:ok, query, {types, ref}, conn}
    end
  end

  @impl true
  def handle_fetch(_query, {types, ref}, opts, conn) do
    case HTTP.recv(conn, 0, timeout(conn, opts)) do
      {:ok, conn, responses} ->
        {halt_or_cont(responses, ref), {:stream, types, responses}, conn}

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, conn}
    end
  end

  defp halt_or_cont([{:done, ref}], ref), do: :halt

  defp halt_or_cont([{tag, ref, _data} | rest], ref) when tag in [:data, :status, :headers] do
    halt_or_cont(rest, ref)
  end

  defp halt_or_cont([], _ref), do: :cont

  @impl true
  def handle_deallocate(_query, _ref, _opts, conn) do
    case HTTP.open_request_count(conn) do
      0 ->
        {:ok, [], conn}

      1 ->
        {:disconnect, Error.exception("cannot stop stream before receiving full response"), conn}
    end
  end

  @impl true
  def handle_execute(%Query{command: :insert} = query, params, opts, conn) do
    {query_params, extra_headers, body} = params

    path = path(conn, query_params, opts)
    headers = headers(conn, extra_headers, opts)

    result =
      if is_function(body, 2) do
        request_chunked(conn, "POST", path, headers, body, opts)
      else
        request(conn, "POST", path, headers, body, opts)
      end

    with {:ok, conn, responses} <- result do
      {:ok, query, responses, conn}
    end
  end

  def handle_execute(query, params, opts, conn) do
    {query_params, extra_headers, body} = params

    path = path(conn, query_params, opts)
    headers = headers(conn, extra_headers, opts)

    with {:ok, conn, responses} <- request(conn, "POST", path, headers, body, opts) do
      {:ok, query, responses, conn}
    end
  end

  @impl true
  def disconnect(_error, conn) do
    {:ok = ok, _conn} = HTTP.close(conn)
    ok
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

          {:error, Error.exception(code: code, message: message), conn}
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

  defp handle_responses([{:done, ref}], ref, acc) do
    {:ok, :lists.reverse(acc)}
  end

  defp handle_responses([{tag, ref, data} | rest], ref, acc)
       when tag in [:data, :status, :headers] do
    handle_responses(rest, ref, [data | acc])
  end

  defp handle_responses([], _ref, acc), do: {:more, acc}

  defp maybe_put_private(conn, _k, nil), do: conn
  defp maybe_put_private(conn, k, v), do: HTTP.put_private(conn, k, v)

  defp timeout(conn), do: HTTP.get_private(conn, :timeout)
  defp timeout(conn, opts), do: Keyword.get(opts, :timeout) || timeout(conn)

  defp settings(conn, opts) do
    default_settings = HTTP.get_private(conn, :settings, [])
    opts_settings = Keyword.get(opts, :settings, [])
    Keyword.merge(default_settings, opts_settings)
  end

  defp headers(conn, extra_headers, opts) do
    extra_headers
    |> maybe_put_header("x-clickhouse-user", get_opts_or_private(conn, opts, :username))
    |> maybe_put_header("x-clickhouse-key", get_opts_or_private(conn, opts, :password))
    |> maybe_put_header("x-clickhouse-database", get_opts_or_private(conn, opts, :database))
  end

  defp get_opts_or_private(conn, opts, key) do
    Keyword.get(opts, key) || HTTP.get_private(conn, key)
  end

  defp maybe_put_header(headers, _k, nil), do: headers
  defp maybe_put_header(headers, k, v), do: [{k, v} | headers]

  defp get_header(headers, key) do
    case List.keyfind(headers, key, 0) do
      {_, value} -> value
      nil = not_found -> not_found
    end
  end

  defp path(conn, query_params, opts) do
    settings = settings(conn, opts)
    "/?" <> URI.encode_query(settings ++ query_params)
  end

  @server_display_name_key :server_display_name

  @spec ensure_same_server(conn, Mint.Types.headers()) :: conn
  defp ensure_same_server(conn, headers) do
    expected_name = HTTP.get_private(conn, @server_display_name_key)
    actual_name = get_header(headers, "x-clickhouse-server-display-name")

    cond do
      expected_name && actual_name ->
        unless actual_name == expected_name do
          Logger.warning(
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
end
