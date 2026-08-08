defmodule Ch.FaultsTest do
  use ExUnit.Case, async: true

  test "returns transport errors when ClickHouse is unreachable" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary])
    {:ok, port} = :inet.port(listen)
    :ok = :gen_tcp.close(listen)
    {:ok, pool} = Ch.start_link(url: "http://localhost:#{port}")

    assert {:error, %Mint.TransportError{reason: reason}} =
             Ch.query(pool, "select 1", %{}, timeout: 100)

    assert reason in [:econnrefused, :closed]
  end

  test "removes a timed out connection and reconnects on the next query" do
    proxy = Help.create_toxiproxy("clickhouse:8123")
    {:ok, pool} = Ch.start_link(url: proxy.url)

    Help.add_toxic(proxy, %{
      "name" => "timeout",
      "type" => "timeout",
      "stream" => "downstream",
      "attributes" => %{"timeout" => 500}
    })

    assert {:error, %Mint.TransportError{reason: :timeout}} =
             Ch.query(pool, "select 1 + 1", %{}, timeout: 100)

    Help.remove_toxic(proxy, "timeout")

    assert {:ok, %{rows: [[2]]}} = Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)
  end

  test "removes a reset connection and reconnects on the next query" do
    proxy = Help.create_toxiproxy("clickhouse:8123")
    {:ok, pool} = Ch.start_link(url: proxy.url)

    Help.add_toxic(proxy, %{
      "name" => "reset_peer",
      "type" => "reset_peer",
      "stream" => "downstream",
      "attributes" => %{"timeout" => 0}
    })

    assert {:error, %Mint.TransportError{}} =
             Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)

    Help.remove_toxic(proxy, "reset_peer")

    assert {:ok, %{rows: [[2]]}} = Ch.query(pool, "select 1 + 1", %{}, timeout: 1_000)
  end
end
