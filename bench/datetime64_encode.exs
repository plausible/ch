defmodule Bench do
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
  }
)
