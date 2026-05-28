defmodule Ch.RowBinaryNetworkTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  property "IPv4 params accept text and decode to tuples", %{pool: pool} do
    check all {text, tuple} <- ipv4_param() do
      assert Ch.query!(pool, "SELECT {value:IPv4}, toString({value:IPv4})", %{
               "value" => text
             }).rows == [[tuple, text]]
    end
  end

  property "IPv6 params accept text and decode to tuples", %{pool: pool} do
    check all {text, tuple, canonical} <- ipv6_param() do
      assert Ch.query!(pool, "SELECT {value:IPv6}, toString({value:IPv6})", %{
               "value" => text
             }).rows == [[tuple, canonical]]
    end
  end

  property "network arrays round-trip as query params through ClickHouse", %{pool: pool} do
    check all ipv4s <- list_of(ipv4_param(), max_length: 8),
              ipv6s <- list_of(ipv6_param(), max_length: 8) do
      ipv4_texts = Enum.map(ipv4s, fn {text, _tuple} -> text end)
      ipv4_tuples = Enum.map(ipv4s, fn {_text, tuple} -> tuple end)
      ipv6_texts = Enum.map(ipv6s, fn {text, _tuple, _canonical} -> text end)
      ipv6_tuples = Enum.map(ipv6s, fn {_text, tuple, _canonical} -> tuple end)

      assert Ch.query!(
               pool,
               "SELECT {ipv4s:Array(IPv4)}, {ipv6s:Array(IPv6)}",
               %{"ipv4s" => ipv4_texts, "ipv6s" => ipv6_texts}
             ).rows == [[ipv4_tuples, ipv6_tuples]]
    end
  end

  test "query params cover nullable, empty arrays, and invalid network values", %{pool: pool} do
    assert Ch.query!(
             pool,
             """
             SELECT
               {ipv4:IPv4},
               {ipv6:IPv6},
               {nullable4:Nullable(IPv4)},
               {nullable6:Nullable(IPv6)},
               {empty4:Array(IPv4)},
               {empty6:Array(IPv6)}
             """,
             %{
               "ipv4" => "127.0.0.1",
               "ipv6" => "::1",
               "nullable4" => nil,
               "nullable6" => nil,
               "empty4" => [],
               "empty6" => []
             }
           ).rows == [[{127, 0, 0, 1}, {0, 0, 0, 0, 0, 0, 0, 1}, nil, nil, [], []]]

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:IPv4}", %{"value" => "999.0.0.1"})

    assert message =~ "IPv4"

    assert {:error, %Ch.Error{message: message}} =
             Ch.query(pool, "SELECT {value:IPv6}", %{"value" => "not-ipv6"})

    assert message =~ "IPv6"
  end

  property "RowBinary network inserts round-trip through ClickHouse", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_network_property (
      id UInt8,
      ipv4 IPv4,
      ipv6 IPv6
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_network_property") end)

    check all rows <- rowbinary_network_rows() do
      Ch.query!(pool, "TRUNCATE TABLE row_binary_network_property")

      rowbinary = RowBinary.encode_rows(rows, ["UInt8", "IPv4", "IPv6"])
      Ch.query!(pool, ["INSERT INTO row_binary_network_property FORMAT RowBinary\n" | rowbinary])

      assert Ch.query!(pool, "SELECT * FROM row_binary_network_property ORDER BY id").rows ==
               Enum.sort_by(rows, &List.first/1)
    end
  end

  test "RowBinary inserts cover nullable, arrays, tuples, and defaults", %{pool: pool} do
    Help.query!("""
    CREATE TABLE row_binary_network_representative (
      id UInt8,
      ipv4 IPv4,
      ipv6 IPv6,
      nullable4 Nullable(IPv4),
      nullable6 Nullable(IPv6),
      ipv4s Array(IPv4),
      ipv6s Array(IPv6),
      pair Tuple(IPv4, IPv6)
    ) ENGINE Memory
    """)

    on_exit(fn -> Help.query!("DROP TABLE row_binary_network_representative") end)

    ipv4_1 = {127, 0, 0, 1}
    ipv4_2 = {192, 168, 1, 1}
    ipv6_1 = {0, 0, 0, 0, 0, 0, 0, 1}
    ipv6_2 = {0x2001, 0x4860, 0x4860, 0, 0, 0, 0, 0x8888}

    rows = [
      [1, ipv4_1, ipv6_1, nil, nil, [], [], {ipv4_1, ipv6_1}],
      [2, nil, nil, ipv4_2, ipv6_2, [ipv4_1, ipv4_2], [ipv6_1, ipv6_2], {ipv4_2, ipv6_2}]
    ]

    types = [
      "UInt8",
      "IPv4",
      "IPv6",
      "Nullable(IPv4)",
      "Nullable(IPv6)",
      "Array(IPv4)",
      "Array(IPv6)",
      "Tuple(IPv4, IPv6)"
    ]

    rowbinary = RowBinary.encode_rows(rows, types)

    Ch.query!(pool, [
      "INSERT INTO row_binary_network_representative FORMAT RowBinary\n" | rowbinary
    ])

    assert Ch.query!(pool, "SELECT * FROM row_binary_network_representative ORDER BY id").rows ==
             [
               [1, ipv4_1, ipv6_1, nil, nil, [], [], {ipv4_1, ipv6_1}],
               [
                 2,
                 {0, 0, 0, 0},
                 {0, 0, 0, 0, 0, 0, 0, 0},
                 ipv4_2,
                 ipv6_2,
                 [ipv4_1, ipv4_2],
                 [ipv6_1, ipv6_2],
                 {ipv4_2, ipv6_2}
               ]
             ]
  end

  test "RowBinary rejects invalid network values" do
    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["127.0.0.1"]], ["IPv4"])
    end

    assert_raise FunctionClauseError, fn ->
      RowBinary.encode_rows([["::1"]], ["IPv6"])
    end
  end

  defp rowbinary_network_rows do
    gen all ids <- uniq_list_of(integer(0..255), max_length: 16),
            ipv4s <- list_of(ipv4_tuple(), length: length(ids)),
            ipv6s <- list_of(ipv6_tuple(), length: length(ids)) do
      Enum.zip_with([ids, ipv4s, ipv6s], fn [id, ipv4, ipv6] -> [id, ipv4, ipv6] end)
    end
  end

  defp ipv4_param do
    gen all tuple <- ipv4_tuple() do
      {tuple |> :inet.ntoa() |> to_string(), tuple}
    end
  end

  defp ipv6_param do
    gen all tuple <- ipv6_tuple() do
      canonical = tuple |> :inet.ntoa() |> to_string()
      {canonical, tuple, canonical}
    end
  end

  defp ipv4_tuple do
    gen all a <- integer(0..255),
            b <- integer(0..255),
            c <- integer(0..255),
            d <- integer(0..255) do
      {a, b, c, d}
    end
  end

  defp ipv6_tuple do
    gen all a <- integer(0..65_535),
            b <- integer(0..65_535),
            c <- integer(0..65_535),
            d <- integer(0..65_535),
            e <- integer(0..65_535),
            f <- integer(0..65_535),
            g <- integer(0..65_535),
            h <- integer(0..65_535) do
      {a, b, c, d, e, f, g, h}
    end
  end
end
