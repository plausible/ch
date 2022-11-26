defmodule Ch.Protocol do
  @moduledoc false
  use DBConnection
  alias String.Chars.NaiveDateTime
  alias Ch.Error

  @impl true
  def connect(opts) do
    scheme = String.to_existing_atom(opts[:scheme] || "http")
    # TODO or hostname?
    address = opts[:host] || "localhost"
    port = opts[:port] || 8123
    database = opts[:database] || "default"
    # active: once, active: false?
    with {:ok, conn} <- Mint.HTTP1.connect(scheme, address, port, mode: :passive) do
      conn = Mint.HTTP1.put_private(conn, :database, database)
      {:ok, conn}
    end
  end

  # TODO or wrap errors in Ch.Error?
  # TODO should use ref for something?
  @impl true
  def ping(conn) do
    case Mint.HTTP1.request(conn, "GET", "/ping", _headers = [], _body = "") do
      {:ok, conn, ref} ->
        case receive_stream(conn, ref) do
          {:ok, conn, [{:status, ^ref, 200}, _headers | _responses]} ->
            {:ok, conn}

          {:ok, conn, [{:status, ^ref, status}, _headers | _responses]} ->
            {:disconnect, Error.exception("unexpected ping http status: #{status}"), conn}

          {:error, conn, error, _responses} ->
            {:disconnect, error, conn}
        end

      {:error, conn, reason} ->
        {:disconnect, reason, conn}
    end
  end

  @impl true
  def checkout(conn) do
    # TODO does repo (or is it db_connection) retry?
    IO.inspect([pid: self()], label: "Protocol.checkout")

    if Mint.HTTP1.open?(conn) do
      {:ok, conn}
    else
      {:disconnect, Error.exception("connection is closed on checkout"), conn}
    end
  end

  @impl true
  def handle_begin(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_commit(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_rollback(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_status(_opts, conn) do
    {:disconnect, Error.exception("transactions are not supported"), conn}
  end

  @impl true
  def handle_prepare(query, opts, conn) do
    IO.inspect([query: query, opts: opts, pid: self()], label: "Protocol.handle_prepare")
    {:ok, query, conn}
  end

  # TODO allow stream as params?

  @impl true
  def handle_execute(query, params, opts, conn) do
    IO.inspect(
      [query: query, params: params, opts: opts, pid: self()],
      label: "Protocol.handle_execute"
    )

    database = opts[:database] || Mint.HTTP1.get_private(conn, :database) || "default"
    %Ch.Query{statement: statement, command: command} = query

    body =
      case command do
        :insert ->
          # TODO
          # Mint.HTTP1.stream_request_body
          csv =
            params
            |> Stream.map(fn row -> Enum.map(row, fn value -> encode_value_for_csv(value) end) end)
            |> NimbleCSV.RFC4180.dump_to_stream()
            |> Enum.into([])

          [statement | csv]

        _other ->
          [statement]
      end

    path =
      case command do
        :insert ->
          "/"

        _other ->
          qs =
            params
            |> Map.new(fn {k, v} -> {"param_#{k}", v} end)
            |> URI.encode_query()

          "/?" <> qs
      end

    headers = [{"x-clickhouse-database", database}]

    # TODO ok to POST for everything, does it make the query not a readonly?
    case Mint.HTTP1.request(conn, "POST", path, headers, body) do
      {:ok, conn, ref} ->
        # body |> Stream.chunk_every()
        # Mint.HTTP1.stream_request_body(conn, ref, )

        case receive_stream(conn, ref) do
          {:ok, conn, [{:status, ^ref, 200}, _headers | responses]} ->
            {:ok, query, collect_body(responses, ref), conn}

          {:ok, conn, [{:status, ^ref, status}, _headers | _responses]} ->
            {:error, Error.exception("unexpected http status: #{status}"), conn}

          {:error, conn, error, _responses} ->
            {:disconnect, error, conn}
        end

      {:error, conn, reason} ->
        {:disconnect, reason, conn}
    end
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
    Mint.HTTP1.close(conn)
    :ok
  end

  # TODO use ref somehow?
  @spec receive_stream(Mint.HTTP1.t(), reference, [Mint.Types.response()]) ::
          {:ok, Mint.HTTP1.t(), [Mint.Types.response()]}
          | {:error, Mint.HTTP1.t(), Mint.Types.error(), [Mint.Types.response()]}
  defp receive_stream(conn, ref, acc \\ []) do
    timeout = Mint.HTTP1.get_private(conn, :timeout, 5000)

    case Mint.HTTP1.recv(conn, 0, timeout) do
      {:ok, conn, responses} ->
        case handle_responses(responses, ref, acc) do
          {:ok, resp} -> {:ok, conn, resp}
          {:more, acc} -> receive_stream(conn, ref, acc)
        end

      {:error, _conn, _reason, responses} = error ->
        put_elem(error, 3, acc ++ responses)
    end
  end

  # TODO handle rest
  defp handle_responses([{:done, ref} = done], ref, acc) do
    {:ok, :lists.reverse([done | acc])}
  end

  defp handle_responses([{tag, ref, _data} = resp | rest], ref, acc)
       when tag in [:data, :status, :headers] do
    handle_responses(rest, ref, [resp | acc])
  end

  defp handle_responses([], _ref, acc) do
    {:more, acc}
  end

  @spec collect_body([{:data, reference, binary} | {:done, reference}], reference) :: iodata
  defp collect_body([{:data, ref, data} | responses], ref) do
    [data | collect_body(responses, ref)]
  end

  defp collect_body([{:done, ref}], ref) do
    []
  end

  # TODO
  defp encode_value_for_csv(n) when is_number(n), do: n
  defp encode_value_for_csv(b) when is_binary(b), do: b

  defp encode_value_for_csv(l) when is_list(l) do
    ["Array(", l |> Enum.map(&encode_value_for_csv/1) |> Enum.intersperse(","), ")"]
  end

  defp encode_value_for_csv(%s{} = d) when s in [Date, DateTime, NaiveDateTime], do: d
end
