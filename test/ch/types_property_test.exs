defmodule Ch.TypesPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Ch.Types

  property "enum type expressions round-trip generated hostile labels" do
    check all labels <- uniq_list_of(enum_label(), min_length: 1, max_length: 8) do
      type = {:enum16, labels |> Enum.with_index() |> Enum.map(fn {label, i} -> {label, i} end)}
      assert type |> Types.encode() |> IO.iodata_to_binary() |> Types.decode() == type
    end
  end

  defp enum_label do
    codepoints = Enum.to_list(?a..?z) ++ [0, 8, 9, 10, 12, 13, ?', ?\\, ?,, ?=, ?(, ?), ?\s]

    gen all chars <- list_of(member_of(codepoints), max_length: 24) do
      List.to_string(chars)
    end
  end
end
