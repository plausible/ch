defmodule Bench do
  @epoch_gregorian_seconds 62_167_219_200

  def via_unix(seconds) do
    seconds
    |> DateTime.from_unix!()
    |> DateTime.to_naive()
  end

  def via_gregorian(seconds) do
    NaiveDateTime.from_gregorian_seconds(seconds + @epoch_gregorian_seconds)
  end
end

Benchee.run(%{
  "via_unix" => fn -> Enum.each(1..1_000_000, &Bench.via_unix/1) end,
  "via_gregorian" => fn -> Enum.each(1..1_000_000, &Bench.via_gregorian/1) end
})
