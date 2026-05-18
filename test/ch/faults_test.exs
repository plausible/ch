defmodule Ch.FaultsTest do
  use ExUnit.Case, async: true

  @socket_opts [:binary, {:active, true}, {:packet, :raw}]

  setup do
    {:ok, listen} = :gen_tcp.listen(0, @socket_opts)
    {:ok, port} = :inet.port(listen)
    {:ok, listen: listen, port: port}
  end

  test "returns transport errors when ClickHouse is unreachable", %{listen: listen, port: port} do
    :ok = :gen_tcp.close(listen)
    {:ok, pool} = Ch.start_link(url: "http://localhost:#{port}")

    assert {:error, %Mint.TransportError{reason: reason}} =
             Ch.query(pool, "select 1", %{}, timeout: 100)

    assert reason in [:econnrefused, :closed]
  end

  test "removes a timed out connection and reconnects on the next query", ctx do
    %{port: port, listen: listen} = ctx
    {:ok, pool} = Ch.start_link(url: "http://localhost:#{port}")

    clickhouse = connect_clickhouse!()

    select =
      Task.async(fn ->
        Ch.query(pool, "select 1 + 1", %{}, timeout: 500)
      end)

    {:ok, mint} = :gen_tcp.accept(listen)
    :ok = :gen_tcp.send(clickhouse, read_packets(mint))
    :ok = :gen_tcp.send(mint, first_byte(read_packets(clickhouse)))
    :ok = :gen_tcp.close(clickhouse)

    assert {:error, %Mint.TransportError{reason: :timeout}} = Task.await(select)

    select =
      Task.async(fn ->
        Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)
      end)

    clickhouse = connect_clickhouse!()
    {:ok, mint} = :gen_tcp.accept(listen)
    :ok = :gen_tcp.send(clickhouse, read_packets(mint))
    :ok = :gen_tcp.send(mint, read_packets(clickhouse))
    :ok = :gen_tcp.close(clickhouse)

    assert {:ok, %{rows: [[2]]}} = Task.await(select)
  end

  test "removes a closed connection and reconnects on the next query", ctx do
    %{port: port, listen: listen} = ctx
    {:ok, pool} = Ch.start_link(url: "http://localhost:#{port}")

    select =
      Task.async(fn ->
        Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)
      end)

    clickhouse = connect_clickhouse!()
    {:ok, mint} = :gen_tcp.accept(listen)
    :ok = :gen_tcp.send(clickhouse, read_packets(mint))
    :ok = :gen_tcp.send(mint, first_byte(read_packets(clickhouse)))
    :ok = :gen_tcp.close(mint)
    :ok = :gen_tcp.close(clickhouse)

    assert {:error, %Mint.TransportError{reason: :closed}} = Task.await(select)

    select =
      Task.async(fn ->
        Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)
      end)

    clickhouse = connect_clickhouse!()
    {:ok, mint} = :gen_tcp.accept(listen)
    :ok = :gen_tcp.send(clickhouse, read_packets(mint))
    :ok = :gen_tcp.send(mint, read_packets(clickhouse))
    :ok = :gen_tcp.close(clickhouse)

    assert {:ok, %{rows: [[2]]}} = Task.await(select)
  end

  defp connect_clickhouse! do
    {:ok, clickhouse} = :gen_tcp.connect({127, 0, 0, 1}, 8123, @socket_opts)
    clickhouse
  end

  defp read_packets(socket) do
    receive do
      {:tcp, ^socket, packet} -> read_packets(socket, packet)
      {:tcp_closed, ^socket} -> ""
    end
  end

  defp read_packets(socket, acc) do
    receive do
      {:tcp, ^socket, packet} -> read_packets(socket, [acc | packet])
      {:tcp_closed, ^socket} -> acc
    after
      50 -> acc
    end
  end

  defp first_byte(binary) do
    :binary.part(IO.iodata_to_binary(binary), 0, 1)
  end
end
