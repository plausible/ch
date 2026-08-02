defmodule Ch.BFloat16EncodingTest do
  use ExUnit.Case, async: true

  import Ch.RowBinary, only: [encode: 2]

  test "rounds Float32 values to nearest BFloat16 with ties to even" do
    # Below halfway: truncate.
    assert encode(:bf16, float32(0x3F80_7FFF)) == <<0x80, 0x3F>>

    # Exact halfway with an even retained bit: round down.
    assert encode(:bf16, float32(0x3F80_8000)) == <<0x80, 0x3F>>

    # Above halfway: round up.
    assert encode(:bf16, float32(0x3F80_8001)) == <<0x81, 0x3F>>

    # Exact halfway with an odd retained bit: round up to even.
    assert encode(:bf16, float32(0x3F81_8000)) == <<0x82, 0x3F>>
  end

  defp float32(bits) do
    <<value::32-float>> = <<bits::32>>
    value
  end
end
