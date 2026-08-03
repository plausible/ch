defmodule Ch.RowBinaryNetworkTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.RowBinary

  property "IPv4 octets round-trip without reordering or truncation" do
    check all octets <- fixed_list(List.duplicate(integer(0..255), 4)) do
      address = List.to_tuple(octets)
      assert RowBinary.decode_rows(encoded(:ipv4, address), [:ipv4]) == [[address]]
    end
  end

  property "IPv6 segments preserve every 16-bit value" do
    check all bytes <- binary(length: 16) do
      address =
        bytes |> :binary.bin_to_list() |> Enum.chunk_every(2) |> Enum.map(&decode_segment/1)

      address = List.to_tuple(address)

      assert encoded(:ipv6, address) == bytes
      assert RowBinary.decode_rows(bytes, [:ipv6]) == [[address]]
    end
  end

  test "rejects invalid IPv4 octets instead of returning invalid iodata" do
    for invalid <- [-1, 256, "1", nil] do
      assert_raise ArgumentError, ~r/invalid IPv4/, fn ->
        RowBinary.encode(:ipv4, {127, 0, 0, invalid})
      end
    end
  end

  test "rejects invalid IPv6 segments instead of truncating them" do
    for invalid <- [-1, 65_536, "1", nil] do
      assert_raise ArgumentError, ~r/invalid IPv6/, fn ->
        RowBinary.encode(:ipv6, {0, 0, 0, 0, 0, 0, 0, invalid})
      end
    end
  end

  defp decode_segment([high, low]), do: high * 256 + low

  defp encoded(type, value) do
    type |> RowBinary.encode(value) |> IO.iodata_to_binary()
  end
end
