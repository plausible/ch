defmodule Ch.Protocol do
  @moduledoc false
  use DBConnection
  alias Ch.Error

  @impl true
  def connect(opts) do
    scheme = String.to_existing_atom(opts[:scheme] || "http")
    # TODO or hostname?
    address = opts[:host] || "localhost"
    port = opts[:port] || 8123
    # active: once, active: false?
    Mint.HTTP1.connect(scheme, address, port)
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
  def handle_prepare(query, _opts, conn) do
    {:ok, query, conn}
  end

  @impl true
  def handle_execute(query, _params, _opts, conn) do
    # TODO
    {:ok, query, _result = nil, conn}
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

  defp receive_stream(conn, ref) do
    receive do
      {:rest, responses} -> receive_stream(conn, ref, responses)
    after
      0 -> receive_stream(conn, ref, [])
    end
  end

  # TODO use ref somehow?
  @spec receive_stream(Mint.HTTP1.t(), reference, [Mint.Types.response()]) ::
          {:ok, Mint.HTTP1.t(), [Mint.Types.response()]}
          | {:error, Mint.HTTP1.t(), Mint.Types.error(), [Mint.Types.response()]}
  defp receive_stream(conn, ref, acc) do
    socket = Mint.HTTP1.get_socket(conn)
    timeout = Mint.HTTP1.get_private(conn, :timeout, :timer.seconds(15))

    receive do
      {tag, ^socket, _data} = message when tag in [:tcp, :ssl] ->
        {:ok, conn, responses} = Mint.HTTP1.stream(conn, message)
        maybe_done(conn, ref, acc ++ responses)

      {tag, ^socket} = message when tag in [:tcp_closed, :ssl_closed] ->
        {:ok, conn, responses} = Mint.HTTP1.stream(conn, message)
        maybe_done(conn, ref, acc ++ responses)

      {tag, ^socket, _reason} = message when tag in [:tcp_error, :ssl_error] ->
        {:error, _conn, _reason, responses} = error = Mint.HTTP1.stream(conn, message)
        put_elem(error, 3, acc ++ responses)
    after
      timeout ->
        error = %Mint.TransportError{reason: :timeout}
        {:error, conn, error, acc}
    end
  end

  defp maybe_done(conn, ref, responses) do
    all_and_rest = Enum.split_while(responses, &(not match?({:done, _}, &1)))

    case all_and_rest do
      {all, []} ->
        receive_stream(conn, ref, all)

      {all, [done | rest]} ->
        # msg queue vs conn.private vs custom state [conn | buffer], what's faster?
        if rest != [], do: send(self(), {:rest, rest})
        {:ok, conn, all ++ [done]}
    end
  end

  # @spec collect_body([{:data, reference, binary} | {:done, reference}], reference) :: iodata
  # defp collect_body([{:data, ref, data} | responses], ref) do
  #   [data | collect_body(responses, ref)]
  # end

  # defp collect_body([{:done, ref}], ref) do
  #   []
  # end
end
