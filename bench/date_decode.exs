defmodule Bench do
  @epoch_date ~D[1970-01-01]
  @epoch_gregorian_days 719_528

  def add(days) do
    Date.add(@epoch_date, days)
  end

  def gregorian(days) do
    Date.from_gregorian_days(days + @epoch_gregorian_days)
  end
end

Benchee.run(
  %{
    "add" => fn -> Enum.each(1..1_000_000, &Bench.add/1) end,
    "gregorian" => fn -> Enum.each(1..1_000_000, &Bench.gregorian/1) end
  },
  profile_after: true
)
