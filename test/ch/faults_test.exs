defmodule Ch.FaultsTest do
  use ExUnit.Case
  import Ch.Test, only: [intercept_packets: 1]

  defp capture_async_log(f) do
    ExUnit.CaptureLog.capture_log([async: true], f)
  end

  @socket_opts [:binary, {:active, true}, {:packet, :raw}]

  setup do
    {:ok, clickhouse} = :gen_tcp.connect({127, 0, 0, 1}, 8123, @socket_opts)
    {:ok, listen} = :gen_tcp.listen(0, @socket_opts)
    {:ok, port} = :inet.port(listen)
    {:ok, clickhouse: clickhouse, listen: listen, port: port}
  end

  describe "connect/1" do
    test "reconnects to eventually reachable server", ctx do
      %{listen: listen, port: port, clickhouse: clickhouse} = ctx

      # make the server unreachable
      :ok = :gen_tcp.close(listen)
      test = self()

      {:ok, conn} = Ch.start_link(port: port, queue_interval: 100)

      log =
        capture_async_log(fn ->
          assert {:error, %DBConnection.ConnectionError{reason: :queue_timeout}} =
                   Ch.query(conn, "select 1 + 1")

          # make the server reachable
          {:ok, listen} = :gen_tcp.listen(port, @socket_opts)
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

      assert log =~ "failed to connect: ** (Mint.TransportError) connection refused"
    end
  end

  describe "ping/1" do
    test "reconnects after timeout", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, timeout: 100, idle_interval: 20)

          {:ok, mint} = :gen_tcp.accept(listen)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))

          {:ok, mint} = :gen_tcp.accept(listen)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) timeout"
    end

    test "reconnects after close", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, idle_interval: 40)

          {:ok, mint} = :gen_tcp.accept(listen)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))
          :ok = :gen_tcp.close(mint)

          {:ok, mint} = :gen_tcp.accept(listen)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end
  end

  describe "query" do
    test "reconnects after timeout", %{port: port, listen: listen, clickhouse: clickhouse} do
      test = self()

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port, timeout: 100)
          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :timeout}} =
                     Ch.query(conn, "select 1 + 1")
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))

          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) timeout"
    end

    test "reconnects after closed on response", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx
      test = self()

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port)
          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "select 1 + 1")
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))
          :ok = :gen_tcp.close(mint)

          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    # TODO non-chunked request

    test "reconects after closed before streaming request", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      test = self()
      rows = [[1, 2], [3, 4]]
      stream = Stream.map(rows, fn row -> Ch.RowBinary.encode_row(row, [:u8, :u8]) end)

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port)
          {:ok, mint} = :gen_tcp.accept(listen)
          :ok = :gen_tcp.close(mint)

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "insert into example(a,b) format RowBinary", stream)
          end)

          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:error, %Ch.Error{code: 60, message: message}} =
                     Ch.query(conn, "insert into example(a,b) format RowBinary", stream)

            assert message =~ ~r/UNKNOWN_TABLE/

            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    test "reconnects after closed while streaming request", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      test = self()
      rows = [[1, 2], [3, 4]]
      stream = Stream.map(rows, fn row -> Ch.RowBinary.encode_row(row, [:u8, :u8]) end)

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port)
          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "insert into example(a,b) format RowBinary", stream)
          end)

          assert_receive {:tcp, ^mint, _packet}
          :ok = :gen_tcp.close(mint)

          {:ok, mint} = :gen_tcp.accept(listen)

          spawn_link(fn ->
            assert {:error, %Ch.Error{code: 60, message: message}} =
                     Ch.query(conn, "insert into example(a,b) format RowBinary", stream)

            assert message =~ ~r/UNKNOWN_TABLE/

            send(test, :done)
          end)

          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end
  end

  defp first_byte(binary) do
    :binary.part(binary, 0, 1)
  end
end
