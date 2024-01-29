defmodule Ch.Connection do
  @moduledoc false
  use DBConnection
  require Logger
  alias Ch.{Error, Query, Result}
  alias Mint.HTTP1, as: HTTP

  @user_agent "ch/" <> Mix.Project.config()[:version]

  @typep conn :: HTTP.t()

  @impl true
  @spec connect([Ch.start_option()]) :: {:ok, conn} | {:error, Error.t() | Mint.Types.error()}
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
    headers = [{"user-agent", @user_agent}]

    case request(conn, "GET", "/ping", headers, _body = "", _opts = []) do
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
    %Query{command: command} = query
    {query_params, extra_headers, body} = params

    path = path(conn, query_params, opts)
    headers = headers(conn, extra_headers, opts)

    with {:ok, conn, _ref} <- send_request(conn, "POST", path, headers, body),
         {:ok, conn} <- eat_ok_status_and_headers(conn, timeout(conn, opts)) do
      {:ok, query, %Result{command: command}, conn}
    end
  end

  @spec eat_ok_status_and_headers(conn, timeout) ::
          {:ok, %{conn: conn, buffer: [Mint.Types.response()]}}
          | {:error, Ch.Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  defp eat_ok_status_and_headers(conn, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case eat_ok_status_and_headers(responses) do
          {:ok, data} ->
            {:ok, %{conn: conn, buffer: data}}

          :more ->
            eat_ok_status_and_headers(conn, timeout)

          :error ->
            all_responses_result =
              case handle_all_responses(responses, []) do
                {:ok, responses} -> {:ok, conn, responses}
                {:more, acc} -> recv_all(conn, acc, timeout)
              end

            with {:ok, conn, responses} <- all_responses_result do
              [_status, headers | data] = responses
              message = IO.iodata_to_binary(data)

              code =
                if code = get_header(headers, "x-clickhouse-exception-code") do
                  String.to_integer(code)
                end

              {:error, Error.exception(code: code, message: message), conn}
            end
        end

      {:error, conn, error, _responses} ->
        {:disconnect, error, conn}
    end
  end

  defp eat_ok_status_and_headers([{:status, _ref, 200} | rest]) do
    eat_ok_status_and_headers(rest)
  end

  defp eat_ok_status_and_headers([{:status, _ref, _status} | _rest]), do: :error
  defp eat_ok_status_and_headers([{:headers, _ref, _headers} | data]), do: {:ok, data}
  defp eat_ok_status_and_headers([]), do: :more

  @impl true
  def handle_fetch(query, result, opts, %{conn: conn, buffer: buffer}) do
    case buffer do
      [] -> handle_fetch(query, result, opts, conn)
      _not_empty -> {halt_or_cont(buffer), %Result{result | data: extract_data(buffer)}, conn}
    end
  end

  def handle_fetch(_query, result, opts, conn) do
    case HTTP.recv(conn, 0, timeout(conn, opts)) do
      {:ok, conn, responses} ->
        {halt_or_cont(responses), %Result{result | data: extract_data(responses)}, conn}

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, conn}
    end
  end

  defp halt_or_cont([{:done, _ref}]), do: :halt
  defp halt_or_cont([_ | rest]), do: halt_or_cont(rest)
  defp halt_or_cont([]), do: :cont

  defp extract_data([{:data, _ref, data} | rest]), do: [data | extract_data(rest)]
  defp extract_data([] = empty), do: empty
  defp extract_data([{:done, _ref}]), do: []

  @impl true
  def handle_deallocate(_query, result, _opts, conn) do
    case HTTP.open_request_count(conn) do
      0 ->
        # TODO data: [], anything else?
        {:ok, %Result{result | data: []}, conn}

      1 ->
        {:disconnect, Error.exception("cannot stop stream before receiving full response"), conn}
    end
  end

  @impl true
  def handle_execute(%Query{} = query, {:stream, params}, opts, conn) do
    {query_params, extra_headers, body} = params

    path = path(conn, query_params, opts)
    headers = headers(conn, extra_headers, opts)

    with {:ok, conn, ref} <- send_request(conn, "POST", path, headers, :stream) do
      case HTTP.stream_request_body(conn, ref, body) do
        {:ok, conn} -> {:ok, query, ref, conn}
        {:error, conn, reason} -> {:disconnect, reason, conn}
      end
    end
  end

  def handle_execute(%Query{} = query, {:stream, ref, body}, opts, conn) do
    case HTTP.stream_request_body(conn, ref, body) do
      {:ok, conn} ->
        case body do
          :eof ->
            with {:ok, conn, responses} <- receive_full_response(conn, timeout(conn, opts)) do
              {:ok, query, responses, conn}
            end

          _other ->
            {:ok, query, ref, conn}
        end

      {:error, conn, reason} ->
        {:disconnect, reason, conn}
    end
  end

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

  @spec request(conn, binary, binary, Mint.Types.headers(), iodata, [Ch.query_option()]) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  defp request(conn, method, path, headers, body, opts) do
    with {:ok, conn, _ref} <- send_request(conn, method, path, headers, body) do
      receive_full_response(conn, timeout(conn, opts))
    end
  end

  @spec request_chunked(conn, binary, binary, Mint.Types.headers(), Enumerable.t(), Keyword.t()) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  def request_chunked(conn, method, path, headers, stream, opts) do
    with {:ok, conn, ref} <- send_request(conn, method, path, headers, :stream),
         {:ok, conn} <- stream_body(conn, ref, stream),
         do: receive_full_response(conn, timeout(conn, opts))
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

  @spec receive_full_response(conn, timeout) ::
          {:ok, conn, [response]}
          | {:error, Error.t(), conn}
          | {:disconnect, Mint.Types.error(), conn}
  defp receive_full_response(conn, timeout) do
    with {:ok, conn, responses} <- recv_all(conn, [], timeout) do
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

  @spec recv_all(conn, [response], timeout()) ::
          {:ok, conn, [response]} | {:disconnect, Mint.Types.error(), conn}
  defp recv_all(conn, acc, timeout) do
    case HTTP.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_all_responses(responses, acc) do
          {:ok, responses} -> {:ok, conn, responses}
          {:more, acc} -> recv_all(conn, acc, timeout)
        end

      {:error, conn, reason, _responses} ->
        {:disconnect, reason, conn}
    end
  end

  for tag <- [:data, :status, :headers] do
    defp handle_all_responses([{unquote(tag), _ref, data} | rest], acc) do
      handle_all_responses(rest, [data | acc])
    end
  end

  defp handle_all_responses([{:done, _ref}], acc), do: {:ok, :lists.reverse(acc)}
  defp handle_all_responses([], acc), do: {:more, acc}

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
    |> maybe_put_new_header("x-clickhouse-user", get_opts_or_private(conn, opts, :username))
    |> maybe_put_new_header("x-clickhouse-key", get_opts_or_private(conn, opts, :password))
    |> maybe_put_new_header("x-clickhouse-database", get_opts_or_private(conn, opts, :database))
    |> maybe_put_new_header("user-agent", @user_agent)
  end

  defp get_opts_or_private(conn, opts, key) do
    Keyword.get(opts, key) || HTTP.get_private(conn, key)
  end

  defp maybe_put_new_header(headers, _name, _no_value = nil), do: headers

  defp maybe_put_new_header(headers, name, value) do
    if List.keymember?(headers, name, 0) do
      headers
    else
      [{name, value} | headers]
    end
  end

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
