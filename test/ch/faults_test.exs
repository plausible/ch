defmodule Ch.FaultsTest do
  use ExUnit.Case

  defp intercept_packets(socket, timeout \\ 100) do
    receive do
      {:tcp, ^socket, packet} ->
        [packet | intercept_packets(socket, timeout)]
    after
      timeout -> []
    end
  end

  defp capture_async_log(f) do
    ExUnit.CaptureLog.capture_log([async: true], f)
  end

  defp first_byte(binary) do
    :binary.part(binary, 0, 1)
  end

  @socket_opts [:binary, {:active, true}, {:packet, :raw}]

  setup do
    {:ok, clickhouse} = :gen_tcp.connect({127, 0, 0, 1}, 8123, @socket_opts)
    {:ok, listen} = :gen_tcp.listen(0, @socket_opts)
    {:ok, port} = :inet.port(listen)
    {:ok, clickhouse: clickhouse, listen: listen, port: port}
  end

  describe "connect/1" do
    test "timeouts and errors on unreachable port", %{listen: listen, port: port} do
      :ok = :gen_tcp.close(listen)

      log =
        capture_async_log(fn ->
          assert {:ok, conn} = Ch.start_link(port: port, queue_interval: 100)

          assert {:error, %DBConnection.ConnectionError{reason: :queue_timeout}} =
                   Ch.query(conn, "select 1 + 1")
        end)

      assert log =~ "failed to connect: ** (Mint.TransportError) connection refused"
    end
  end

  describe "ping/1" do
    test "disconnects on timeout", %{port: port, listen: listen, clickhouse: clickhouse} do
      Ch.start_link(port: port, timeout: 100, idle_interval: 20)

      {:ok, mint} = :gen_tcp.accept(listen)

      log =
        capture_async_log(fn ->
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          [first | _rest] = intercept_packets(clickhouse)
          :ok = :gen_tcp.send(mint, first_byte(first))

          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) timeout"
    end

    test "disconnects on closed", %{port: port, listen: listen, clickhouse: clickhouse} do
      Ch.start_link(port: port, idle_interval: 20)
      {:ok, mint} = :gen_tcp.accept(listen)

      log =
        capture_async_log(fn ->
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          [first | _rest] = intercept_packets(clickhouse)
          :ok = :gen_tcp.send(mint, first_byte(first))
          :ok = :gen_tcp.close(mint)

          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end
  end

  describe "query" do
    test "timeouts on slow response", %{port: port, listen: listen, clickhouse: clickhouse} do
      {:ok, conn} = Ch.start_link(port: port, timeout: 100)
      {:ok, mint} = :gen_tcp.accept(listen)

      test = self()

      log =
        capture_async_log(fn ->
          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :timeout}} =
                     Ch.query(conn, "select 1 + 1")

            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          [first | _rest] = intercept_packets(clickhouse)
          :ok = :gen_tcp.send(mint, first_byte(first))

          assert_receive :done
          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) timeout"
    end

    test "closed when receiving response", %{port: port, listen: listen, clickhouse: clickhouse} do
      {:ok, conn} = Ch.start_link(port: port)
      {:ok, mint} = :gen_tcp.accept(listen)

      test = self()

      log =
        capture_async_log(fn ->
          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "select 1 + 1")

            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          [first | _rest] = intercept_packets(clickhouse)
          :ok = :gen_tcp.send(mint, first_byte(first))
          :ok = :gen_tcp.close(mint)

          assert_receive :done
          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    test "closed when sending data", %{port: port, listen: listen} do
      {:ok, conn} = Ch.start_link(port: port)
      {:ok, mint} = :gen_tcp.accept(listen)

      test = self()

      log =
        capture_async_log(fn ->
          spawn_link(fn ->
            data = Ch.RowBinary.encode_rows([[1, 2], [3, 4]], [:u8, :u8])

            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "insert into table(a,b)", data, format: "RowBinary")

            send(test, :done)
          end)

          assert_receive {:tcp, ^mint, _packet}
          :ok = :gen_tcp.close(mint)

          assert_receive :done
          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    test "reconnects after disconnect", %{port: port, listen: listen, clickhouse: clickhouse} do
      {:ok, conn} = Ch.start_link(port: port)
      {:ok, mint} = :gen_tcp.accept(listen)

      test = self()

      log =
        capture_async_log(fn ->
          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "select 1 + 1")
          end)

          _ = intercept_packets(mint)
          :ok = :gen_tcp.close(mint)

          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          assert_receive :done
          refute_receive _anything
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end
  end
end
