defmodule Ch.FaultsTest do
  alias Ch.Result
  use ExUnit.Case
  import Ch.Test, only: [intercept_packets: 1]

  defp capture_async_log(f) do
    ExUnit.CaptureLog.capture_log([async: true], f)
  end

  @socket_opts [:binary, {:active, true}, {:packet, :raw}]

  setup do
    # this setup makes the test act as MITM for clickhouse and ch's http conn (mint)
    # allowing the test to intercept, slow down, and modify packets to cause failures
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

      {:ok, conn} = Ch.start_link(port: port, queue_interval: 100, backoff_min: 0)

      log =
        capture_async_log(fn ->
          assert {:error, %DBConnection.ConnectionError{reason: :queue_timeout}} =
                   Ch.query(conn, "select 1 + 1")

          # make the server reachable
          {:ok, listen} = :gen_tcp.listen(port, @socket_opts)
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          # select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          assert_receive :done
          refute_receive _anything
        end)

      assert log =~ "failed to connect: ** (Mint.TransportError) connection refused"
    end
  end

  describe "connect/1 handshake" do
    test "reconnects after timeout", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, timeout: 100, backoff_min: 0)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # failed handshake
          handshake = intercept_packets(mint)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          handshake = intercept_packets(mint)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
        end)

      assert log =~ "failed to connect: ** (Mint.TransportError) timeout"
    end

    test "reconnects after closed", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, backoff_min: 0)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # failed handshake
          handshake = intercept_packets(mint)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))
          :gen_tcp.close(mint)

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          handshake = intercept_packets(mint)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
        end)

      assert log =~ "failed to connect: ** (Mint.TransportError) socket closed"
    end

    test "reconnects after unexpected status code", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, backoff_min: 0)

          # connect
          {:ok, mint1} = :gen_tcp.accept(listen)

          # failed handshake
          handshake = intercept_packets(mint1)
          assert handshake =~ "select 1, version()"
          altered_handshake = String.replace(handshake, "select 1", "select x")
          :ok = :gen_tcp.send(clickhouse, altered_handshake)
          :ok = :gen_tcp.send(mint1, intercept_packets(clickhouse))

          # reconnect
          {:ok, mint2} = :gen_tcp.accept(listen)

          # handshake
          handshake = intercept_packets(mint2)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint2, intercept_packets(clickhouse))

          # no socket leak
          refute Port.info(mint1)
          assert Port.info(mint2)
        end)

      assert log =~ "UNKNOWN_IDENTIFIER"
    end

    test "reconnects after incorrect query result", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, backoff_min: 0)

          # connect
          {:ok, mint1} = :gen_tcp.accept(listen)

          # failed handshake
          handshake = intercept_packets(mint1)
          assert handshake =~ "select 1, version()"

          altered_handshake =
            String.replace(handshake, "select 1, version()", "select 2, version()")

          :ok = :gen_tcp.send(clickhouse, altered_handshake)
          :ok = :gen_tcp.send(mint1, intercept_packets(clickhouse))

          # reconnect
          {:ok, mint2} = :gen_tcp.accept(listen)

          # handshake
          handshake = intercept_packets(mint2)
          assert handshake =~ "select 1, version()"
          :ok = :gen_tcp.send(clickhouse, handshake)
          :ok = :gen_tcp.send(mint2, intercept_packets(clickhouse))

          # no socket leak
          refute Port.info(mint1)
          assert Port.info(mint2)
        end)

      assert log =~ "failed to connect: ** (Ch.Error) unexpected result for 'select 1, version()'"
    end
  end

  describe "ping/1" do
    test "reconnects after timeout", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, timeout: 100, idle_interval: 20)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          # failed ping
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          # ping
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) timeout"
    end

    test "reconnects after close", %{port: port, listen: listen, clickhouse: clickhouse} do
      log =
        capture_async_log(fn ->
          Ch.start_link(port: port, idle_interval: 40)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          # failed ping
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))
          :ok = :gen_tcp.close(mint)

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          # ping
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

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :timeout}} =
                     Ch.query(conn, "select 1 + 1")
          end)

          # failed select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          # select 1 + 1
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

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(conn, "select 1 + 1")
          end)

          # failed select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, first_byte(intercept_packets(clickhouse)))
          :ok = :gen_tcp.close(mint)

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          # select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))
          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    test "reconnects after Connection: close response from server", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx
      test = self()

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
            send(test, :done)
          end)

          # first select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          response =
            String.replace(
              intercept_packets(clickhouse),
              "Connection: Keep-Alive",
              "Connection: Close"
            )

          :ok = :gen_tcp.send(mint, response)
          :ok = :gen_tcp.close(mint)
          assert_receive :done

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %{num_rows: 1, rows: [[2]]}} =
                     Ch.query(conn, "select 1 + 1")

            send(test, :done)
          end)

          # select 1 + 1
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          assert_receive :done
        end)

      refute log =~ "disconnected: **"
      assert log =~ "connection was closed by the server"
    end

    # TODO non-chunked request

    test "reconnects after closed before streaming request", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      test = self()
      rows = [[1, 2], [3, 4]]
      stream = Stream.map(rows, fn row -> Ch.RowBinary.encode_row(row, [:u8, :u8]) end)

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(database: Ch.Test.database(), port: port)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          # disconnect before insert
          :ok = :gen_tcp.close(mint)

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(
                       conn,
                       "insert into unknown_table(a,b) format RowBinary",
                       stream,
                       encode: false
                     )
          end)

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:error, %Ch.Error{code: 60, message: message}} =
                     Ch.query(
                       conn,
                       "insert into unknown_table(a,b) format RowBinary",
                       stream,
                       encode: false
                     )

            assert message =~ ~r/UNKNOWN_TABLE/

            send(test, :done)
          end)

          # insert
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
          {:ok, conn} = Ch.start_link(database: Ch.Test.database(), port: port)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:error, %Mint.TransportError{reason: :closed}} =
                     Ch.query(
                       conn,
                       "insert into unknown_table(a,b) format RowBinary",
                       stream,
                       encode: false
                     )
          end)

          # close after first packet from mint arrives
          assert_receive {:tcp, ^mint, _packet}
          :ok = :gen_tcp.close(mint)

          # reconnect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:error, %Ch.Error{code: 60, message: message}} =
                     Ch.query(
                       conn,
                       "insert into unknown_table(a,b) format RowBinary",
                       stream,
                       encode: false
                     )

            assert message =~ ~r/UNKNOWN_TABLE/

            send(test, :done)
          end)

          # insert
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          assert_receive :done
        end)

      assert log =~ "disconnected: ** (Mint.TransportError) socket closed"
    end

    test "warns on different server name", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx
      test = self()

      header = "X-ClickHouse-Server-Display-Name"
      %Result{headers: headers} = Ch.Test.query("select 1")
      {_, expected_name} = List.keyfind!(headers, String.downcase(header), 0)

      log =
        capture_async_log(fn ->
          {:ok, conn} = Ch.start_link(port: port)

          # connect
          {:ok, mint} = :gen_tcp.accept(listen)

          # handshake
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
          :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

          spawn_link(fn ->
            assert {:ok, %Result{rows: [[1]]}} = Ch.query(conn, "select 1")
            send(test, :done)
          end)

          # query
          :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

          response =
            String.replace(
              intercept_packets(clickhouse),
              "#{header}: #{expected_name}",
              "#{header}: not-#{expected_name}"
            )

          :ok = :gen_tcp.send(mint, response)

          assert_receive :done
        end)

      assert log =~
               "[warning] Server mismatch detected." <>
                 " Expected \"#{expected_name}\" but got \"not-#{expected_name}\"!" <>
                 " Connection pooling might be unstable."
    end

    test "raises exception when reconnection fails", ctx do
      %{port: port, listen: listen, clickhouse: clickhouse} = ctx

      {:ok, conn} = Ch.start_link(port: port)
      {:ok, mint} = :gen_tcp.accept(listen)

      :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))
      :ok = :gen_tcp.send(mint, intercept_packets(clickhouse))

      spawn_link(fn ->
        assert {:ok, %{num_rows: 1, rows: [[2]]}} = Ch.query(conn, "select 1 + 1")
      end)

      :ok = :gen_tcp.send(clickhouse, intercept_packets(mint))

      response =
        String.replace(
          intercept_packets(clickhouse),
          "Connection: Keep-Alive",
          "Connection: Close"
        )

      :ok = :gen_tcp.send(mint, response)
      :ok = :gen_tcp.close(mint)

      # close the listening socket so reconnection will fail
      :ok = :gen_tcp.close(listen)

      log =
        capture_async_log(fn ->
          assert_raise DBConnection.ConnectionError, ~r/Reconnection failed/, fn ->
            # this should trigger maybe_reconnect
            Ch.query(conn, "select 1 + 1")
          end
        end)

      assert log =~ "Could not reconnect due to %Mint.TransportError{reason: :econnrefused}"
    end
  end

  defp first_byte(binary) do
    :binary.part(binary, 0, 1)
  end
end
