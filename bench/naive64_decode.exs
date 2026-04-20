defmodule Bench do
  @epoch_gregorian_seconds 62_167_219_200

  # current: allocate DateTime, discard it
  def via_unix(ticks, time_unit) do
    ticks
    |> DateTime.from_unix!(time_unit)
    |> DateTime.to_naive()
  end

  # direct: decompose ticks into seconds + sub-second remainder
  def via_gregorian(ticks, time_unit) do
    seconds = div(ticks, time_unit)
    remainder = rem(ticks, time_unit)

    microsecond =
      if time_unit <= 1_000_000 do
        {remainder * div(1_000_000, time_unit), precision(time_unit)}
      else
        {div(remainder, div(time_unit, 1_000_000)), 6}
      end

    NaiveDateTime.from_gregorian_seconds(seconds + @epoch_gregorian_seconds, microsecond)
  end

  @compile inline: [time_unit: 1]
  for precision <- 0..9 do
    time_unit = Integer.pow(10, precision)
    defp time_unit(unquote(precision)), do: unquote(time_unit)
  end

  defp precision(1), do: 0
  defp precision(10), do: 1
  defp precision(100), do: 2
  defp precision(1_000), do: 3
  defp precision(10_000), do: 4
  defp precision(100_000), do: 5
  defp precision(_), do: 6
end

# representative unix millisecond timestamps
millis = Enum.map(1..1_000_000, fn i -> 1_700_000_000_000 + i end)
micros = Enum.map(1..1_000_000, fn i -> 1_700_000_000_000_000 + i end)

Benchee.run(
  %{
    "via_unix ms" => fn -> Enum.each(millis, &Bench.via_unix(&1, 1_000)) end,
    "via_gregorian ms" => fn -> Enum.each(millis, &Bench.via_gregorian(&1, 1_000)) end,
    "via_unix us" => fn -> Enum.each(micros, &Bench.via_unix(&1, 1_000_000)) end,
    "via_gregorian us" => fn -> Enum.each(micros, &Bench.via_gregorian(&1, 1_000_000)) end
  },
  profile_after: true
)
