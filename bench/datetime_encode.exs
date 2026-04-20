defmodule Bench do
  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  {epoch_seconds_since_gregorian, _} = NaiveDateTime.to_gregorian_seconds(@epoch_naive_datetime)
  @epoch_seconds_since_gregorian epoch_seconds_since_gregorian

  def naive_diff(%NaiveDateTime{} = naive) do
    NaiveDateTime.diff(naive, @epoch_naive_datetime)
  end

  def to_utc_to_unix(%NaiveDateTime{} = naive) do
    naive |> DateTime.from_naive!("Etc/UTC") |> DateTime.to_unix()
  end

  def to_gregorian_diff(%NaiveDateTime{} = naive) do
    {seconds, _} = NaiveDateTime.to_gregorian_seconds(naive)
    seconds - @epoch_seconds_since_gregorian
  end
end

Benchee.run(
  %{
    "naive_diff" => &Bench.naive_diff/1,
    "to_utc_to_unix" => &Bench.to_utc_to_unix/1,
    "to_gregorian_diff" => &Bench.to_gregorian_diff/1
  },
  inputs: %{
    "now" => NaiveDateTime.utc_now()
  }
)
