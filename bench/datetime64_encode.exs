defmodule Bench do
<<<<<<< HEAD
  @epoch_gregorian_seconds 62_167_219_200

  def to_unix({time_unit, %DateTime{} = datetime}) do
    DateTime.to_unix(datetime, time_unit)
  end

  def to_gregorian({time_unit, %DateTime{} = datetime}) do
    {seconds, micros} = DateTime.to_gregorian_seconds(datetime)
    (seconds - @epoch_gregorian_seconds) * time_unit + div(micros * time_unit, 1_000_000)
  end
end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

Benchee.run(
  %{
    "to_unix" => &Bench.to_unix/1,
    "to_gregorian" => &Bench.to_gregorian/1
  },
  inputs: %{
    "DateTime64(3) now" => {_time_unit = 1_000, DateTime.utc_now()},
    "DateTime64(6) now" => {_time_unit = 1_000_000, DateTime.utc_now()},
    "DateTime64(9) now" => {_time_unit = 1_000_000_000, DateTime.utc_now()},
    "DateTime64(3) now in Tahiti" =>
      {_time_unit = 1_000, DateTime.shift_zone!(DateTime.utc_now(), "Pacific/Tahiti")},
    "DateTime64(6) now in Tahiti" =>
      {_time_unit = 1_000_000, DateTime.shift_zone!(DateTime.utc_now(), "Pacific/Tahiti")},
    "DateTime64(9) now in Tahiti" =>
      {_time_unit = 1_000_000_000, DateTime.shift_zone!(DateTime.utc_now(), "Pacific/Tahiti")}
=======
  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])
  # NaiveDateTime.to_gregorian_seconds(@epoch_naive_datetime)
  @epoch_seconds_since_gregorian 62_167_219_200

  def naive_diff({time_unit, %NaiveDateTime{} = naive}) do
    NaiveDateTime.diff(naive, @epoch_naive_datetime, time_unit)
  end

  def to_gregorian_diff({time_unit, %NaiveDateTime{} = naive}) do
    {seconds, micros} = NaiveDateTime.to_gregorian_seconds(naive)
    (seconds - @epoch_seconds_since_gregorian) * time_unit + div(micros * time_unit, 1_000_000)
  end
end

Benchee.run(
  %{
    "naive_diff" => &Bench.naive_diff/1,
    "to_gregorian_diff" => &Bench.to_gregorian_diff/1
  },
  inputs: %{
    "DateTime64(3) now" => {_time_unit = 1_000, NaiveDateTime.utc_now()},
    "DateTime64(6) now" => {_time_unit = 1_000_000, NaiveDateTime.utc_now()},
    "DateTime64(9) now" => {_time_unit = 1_000_000_000, NaiveDateTime.utc_now()}
>>>>>>> 37416ba (improve naive datetime handling in rowbinary)
  }
)
