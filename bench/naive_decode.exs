defmodule Bench do
  @epoch_gregorian_seconds 62_167_219_200
  @epoch_naive_datetime ~N[1970-01-01 00:00:00]

  def via_add(seconds) do
    NaiveDateTime.add(@epoch_naive_datetime, seconds)
  end

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
  "via_add" => fn -> Enum.each(1..1_000_000, &Bench.via_add/1) end,
  "via_unix" => fn -> Enum.each(1..1_000_000, &Bench.via_unix/1) end,
  "via_gregorian" => fn -> Enum.each(1..1_000_000, &Bench.via_gregorian/1) end
})
