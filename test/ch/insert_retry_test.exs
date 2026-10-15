defmodule Ch.InsertRetryTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  @moduletag capture_log: true

  alias Mint.HTTP
  import Ch.Test, only: [intercept_packets: 1]

  @socket_opts [:binary, active: true, packet: :raw]

  setup ctx do
    {:ok, query_options: ctx[:query_options] || []}
  end

  describe "lost INSERT acknowledgements" do
    setup do
      {:ok, clickhouse} = :gen_tcp.connect({127, 0, 0, 1}, 8123, @socket_opts)
      {:ok, listen} = :gen_tcp.listen(0, @socket_opts)
      {:ok, port} = :inet.port(listen)

      direct = start_supervised!({Ch, database: Ch.Test.database()}, id: :direct)
      table = "insert_retry_#{System.unique_integer([:positive])}"
      Ch.query!(direct, "CREATE TABLE #{table}(n UInt8) ENGINE Memory")
      on_exit(fn -> Ch.Test.query("DROP TABLE IF EXISTS #{table}") end)

      conn =
        start_supervised!(
          {Ch, port: port, database: Ch.Test.database(), idle_interval: 60_000},
          id: :proxied
        )

      {:ok, conn: conn, direct: direct, table: table, clickhouse: clickhouse, listen: listen}
    end

    test "does not replay an ordinary INSERT", ctx do
      assert_lost_acknowledgement(ctx, fn ->
        Ch.query!(ctx.conn, "INSERT INTO #{ctx.table} VALUES (42)", [], ctx.query_options)
      end)
    end

    test "does not replay an Enumerable RowBinary INSERT", ctx do
      data = Stream.map([[42]], &Ch.RowBinary.encode_row(&1, ["UInt8"]))
      options = Keyword.put(ctx.query_options, :encode, false)

      assert_lost_acknowledgement(ctx, fn ->
        Ch.query!(ctx.conn, "INSERT INTO #{ctx.table} FORMAT RowBinary", data, options)
      end)
    end

    test "does not replay a Collectable INSERT at EOF", ctx do
      chunks = [Ch.RowBinary.encode_row([42], ["UInt8"])]
      options = Keyword.put(ctx.query_options, :encode, false)

      assert_lost_acknowledgement(ctx, fn ->
        DBConnection.run(ctx.conn, fn conn ->
          Enum.into(
            chunks,
            Ch.stream(conn, "INSERT INTO #{ctx.table} FORMAT RowBinary", [], options)
          )
        end)
      end)
    end
  end

  test "an INSERT rejected before transmission can reconnect and retry" do
    {:ok, conn} = HTTP.connect(:http, "localhost", 8123, mode: :passive)
    {:ok, conn} = HTTP.close(conn)
    query = Ch.Query.build("INSERT INTO example VALUES (42)")
    params = DBConnection.Query.encode(query, [], [])

    assert {:disconnect_and_retry, %Mint.HTTPError{reason: :closed}, _conn} =
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

  defp assert_lost_acknowledgement(ctx, insert) do
    {:ok, mint} = :gen_tcp.accept(ctx.listen, 5_000)

    # Forward the handshake.
    :ok = :gen_tcp.send(ctx.clickhouse, intercept_packets(mint))
    :ok = :gen_tcp.send(mint, intercept_packets(ctx.clickhouse))

    task = Task.async(fn -> assert_raise Mint.TransportError, insert end)
    :ok = :gen_tcp.send(ctx.clickhouse, intercept_packets(mint))

    # ClickHouse committed the INSERT, but its successful response never reaches Ch.
    response = intercept_packets(ctx.clickhouse)
    assert response =~ "HTTP/1.1 200"
    assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[1]]
    :ok = :gen_tcp.close(mint)

    assert %Mint.TransportError{reason: :closed} = Task.await(task)
    assert Ch.query!(ctx.direct, "SELECT count(*) FROM #{ctx.table}").rows == [[1]]
  end
end
