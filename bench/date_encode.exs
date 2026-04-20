defmodule Bench do
  @epoch_date ~D[1970-01-01]
  @epoch_gregorian_days 719_528

  def diff(%Date{} = date) do
    Date.diff(date, @epoch_date)
  end

  def gregorian(%Date{} = date) do
    Date.to_gregorian_days(date) - @epoch_gregorian_days
  end
end

Benchee.run(
  %{
    "diff" => fn date ->
      Enum.each(1..1_000_000, fn _ -> Bench.diff(date) end)
    end,
    "gregorian" => fn date ->
      Enum.each(1..1_000_000, fn _ -> Bench.gregorian(date) end)
    end
  },
  inputs: %{"now" => Date.utc_today()},
  profile_after: true
)
