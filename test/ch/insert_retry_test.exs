defmodule Ch.InsertRetryTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  @moduletag capture_log: true

  alias Mint.HTTP

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  describe "insert retries" do
    setup do
      direct = start_supervised!({Ch, database: Ch.Test.database()}, id: :direct)
      table = "insert_retry_#{System.unique_integer([:positive])}"
      Ch.query!(direct, "CREATE TABLE #{table}(n UInt8) ENGINE Memory")
      on_exit(fn -> Ch.Test.query("DROP TABLE IF EXISTS #{table}") end)
      {:ok, direct: direct, table: table}
    end

    test "returns an error without replaying an ordinary INSERT", ctx do
      assert_lost_acknowledgement(ctx, "INSERT INTO #{ctx.table} VALUES (42)")
    end

    test "returns an error without replaying an Enumerable RowBinary INSERT", ctx do
      data = Stream.map([[42]], &Ch.RowBinary.encode_rows([&1], ["UInt8"]))

      assert_lost_acknowledgement(
        ctx,
        "INSERT INTO #{ctx.table} FORMAT RowBinary",
        data,
        encode: false
      )
    end

    test "does not replay an INSERT whose command is not detected", ctx do
      statement = "/* retry regression */ INSERT INTO #{ctx.table} VALUES (42)"
      assert Ch.Query.build(statement).command == nil
      assert_lost_acknowledgement(ctx, statement)
    end

    test "does not trust a read-only command override for an INSERT", ctx do
      assert_lost_acknowledgement(ctx, "INSERT INTO #{ctx.table} VALUES (42)", [],
        command: :select
      )
    end

    test "returns an error without replaying a Collectable INSERT at EOF", ctx do
      chunks = [Ch.RowBinary.encode_rows([[42]], ["UInt8"])]

      assert_lost_acknowledgement(
        ctx,
        "INSERT INTO #{ctx.table} FORMAT RowBinary",
        chunks,
        [encode: false],
        fn conn, statement, chunks, options ->
          try do
            DBConnection.run(conn, fn conn ->
              Enum.into(chunks, Ch.stream(conn, statement, [], options))
            end)
          rescue
            error in Mint.TransportError -> {:error, error}
          end
        end
      )
    end

    test "reconnects before sending an INSERT on a stale idle connection", ctx do
      {:ok, listener} =
        :gen_tcp.listen(0, [:binary, active: false, packet: :raw, ip: {127, 0, 0, 1}])

      {:ok, port} = :inet.port(listener)
      parent = self()
      proxy = Task.async(fn -> stale_idle_proxy(listener, parent) end)
      proxy_pid = proxy.pid

      try do
        conn =
          start_supervised!(
            {Ch,
             hostname: "127.0.0.1",
             port: port,
             database: Ch.Test.database(),
             idle_interval: 60_000,
             backoff_min: 1},
            id: :proxied
          )

        # Ensure the first handshake has been received and the connection is back in the pool.
        assert :ok = DBConnection.run(conn, fn _conn -> :ok end)
        send(proxy_pid, :close_idle_connection)
        assert_receive {:idle_connection_closed, ^proxy_pid}, 5_000

        # Complete another local round trip after the proxy sends FIN, before attempting a write.
        assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[0]]

        statement = "INSERT INTO #{ctx.table} VALUES (42)"
        assert {:ok, %{command: :insert}} = Ch.query(conn, statement, [], ctx.query_options)
        assert_receive {:forwarded_request, ^proxy_pid, request}, 5_000
        assert request =~ statement
        refute_receive {:forwarded_request, ^proxy_pid, _request}
        assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[1]]
      after
        Task.shutdown(proxy, :brutal_kill)
        stop_supervised(:proxied)
        :gen_tcp.close(listener)
      end
    end
  end

  test "an INSERT rejected before transmission can reconnect and retry" do
    {:ok, conn} = HTTP.connect(:http, "localhost", 8123, mode: :passive)
    {:ok, conn} = HTTP.close(conn)
    query = Ch.Query.build("INSERT INTO example VALUES (42)")
    params = DBConnection.Query.encode(query, [], [])

    assert {:disconnect_and_retry, %Mint.TransportError{reason: :closed}, _conn} =
             Ch.Connection.handle_execute(query, params, [], conn)
  end

  test "a streamed INSERT continuation never retries on a closed connection" do
    {:ok, conn} = HTTP.connect(:http, "localhost", 8123, mode: :passive)
    {:ok, conn, ref} = HTTP.request(conn, "POST", "/", [], :stream)
    {:ok, conn} = HTTP.close(conn)
    query = Ch.Query.build("INSERT INTO example FORMAT RowBinary")

    for chunk <- [<<42>>, :eof] do
      assert {:disconnect, %Mint.HTTPError{reason: :closed}, _conn} =
               Ch.Connection.handle_execute(query, {:stream, ref, chunk}, [], conn)
    end
  end

  defp assert_lost_acknowledgement(
         ctx,
         statement,
         params \\ [],
         options \\ [],
         execute \\ &Ch.query/4
       ) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listener)
    parent = self()
    proxy = Task.async(fn -> proxy(listener, parent) end)
    proxy_pid = proxy.pid

    try do
      conn =
        start_supervised!(
          {Ch,
           hostname: "127.0.0.1",
           port: port,
           database: Ch.Test.database(),
           idle_interval: 60_000,
           backoff_min: 1},
          id: :proxied
        )

      options = Keyword.merge(ctx.query_options, options)
      task = Task.async(fn -> execute.(conn, statement, params, options) end)

      assert_receive {:acknowledgement, ^proxy_pid, response}, 5_000
      assert String.starts_with?(response, "HTTP/1.1 200 ")
      assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[1]]
      send(proxy_pid, :drop_acknowledgement)

      assert {:error, %Mint.TransportError{reason: :closed}} = Task.await(task, 10_000)

      # Reconnects still work: the next request must be this read, not a replay of the write.
      assert {:ok, %{rows: [[42]]}} = Ch.query(conn, "SELECT 42", [], ctx.query_options)
      assert_receive {:forwarded_request, ^proxy_pid, request}, 5_000
      assert request =~ "SELECT 42"
      refute request =~ ctx.table
      assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[1]]
    after
      Task.shutdown(proxy, :brutal_kill)
      stop_supervised(:proxied)
      :gen_tcp.close(listener)
    end
  end

  defp proxy(listener, parent) do
    {:ok, server} =
      :gen_tcp.connect({127, 0, 0, 1}, 8123, [:binary, active: false, packet: :raw], 5_000)

    {:ok, client} = :gen_tcp.accept(listener, 5_000)
    forward(client, server)
    forward(server, client)

    # A full successful response establishes that ClickHouse has committed the INSERT.
    forward(client, server)
    response = recv_http(server)
    send(parent, {:acknowledgement, self(), response})

    receive do
      :drop_acknowledgement -> :ok
    after
      5_000 -> raise "timed out waiting to drop the INSERT acknowledgement"
    end

    :ok = :gen_tcp.close(client)
    {:ok, client} = :gen_tcp.accept(listener, 5_000)
    forward(client, server)
    forward(server, client)
    forward_queries(client, server, parent)
  end

  defp stale_idle_proxy(listener, parent) do
    {:ok, server} =
      :gen_tcp.connect({127, 0, 0, 1}, 8123, [:binary, active: false, packet: :raw], 5_000)

    {:ok, client} = :gen_tcp.accept(listener, 5_000)
    forward(client, server)
    forward(server, client)

    receive do
      :close_idle_connection -> :ok
    after
      5_000 -> raise "timed out waiting to close the idle connection"
    end

    :ok = :gen_tcp.close(client)
    send(parent, {:idle_connection_closed, self()})
    {:ok, client} = :gen_tcp.accept(listener, 5_000)
    forward(client, server)
    forward(server, client)
    forward_queries(client, server, parent)
  end

  defp forward_queries(client, server, parent) do
    request = recv_http(client)
    send(parent, {:forwarded_request, self(), request})
    :ok = :gen_tcp.send(server, request)
    forward(server, client)
    forward_queries(client, server, parent)
  end

  defp forward(source, destination) do
    :ok = :gen_tcp.send(destination, recv_http(source))
  end

  defp recv_http(socket) do
    recv_http(socket, <<>>, System.monotonic_time(:millisecond) + 5_000)
  end

  defp recv_http(socket, buffer, deadline) do
    if complete_http?(buffer) do
      buffer
    else
      remaining = deadline - System.monotonic_time(:millisecond)
      if remaining <= 0, do: raise("timed out receiving HTTP message")
      {:ok, packet} = :gen_tcp.recv(socket, 0, remaining)
      recv_http(socket, buffer <> packet, deadline)
    end
  end

  defp complete_http?(buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [headers, body] ->
        headers = String.downcase(headers)

        if String.contains?(headers, "transfer-encoding: chunked") do
          complete_chunks?(body)
        else
          case Regex.run(~r/content-length:\s*(\d+)/, headers) do
            [_, length] -> byte_size(body) == String.to_integer(length)
            nil -> false
          end
        end

      [_incomplete] ->
        false
    end
  end

  defp complete_chunks?(buffer) do
    with [size, rest] <- :binary.split(buffer, "\r\n"),
         {size, _extensions} <- Integer.parse(size, 16) do
      if size == 0 do
        rest == "\r\n"
      else
        case rest do
          <<_chunk::binary-size(^size), "\r\n", rest::binary>> -> complete_chunks?(rest)
          _incomplete -> false
        end
      end
    else
      _incomplete -> false
    end
  end
end
