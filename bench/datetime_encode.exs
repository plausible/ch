defmodule Bench do
  @epoch_gregorian_seconds 62_167_219_200

  def to_unix(%DateTime{} = datetime) do
    DateTime.to_unix(datetime)
  end

  def to_gregorian(%DateTime{} = datetime) do
    {seconds, _} = DateTime.to_gregorian_seconds(datetime)
    seconds - @epoch_gregorian_seconds
  end
end

Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

Benchee.run(
  %{
    "to_unix" => &Bench.to_unix/1,
    "to_gregorian" => &Bench.to_gregorian/1
  },
  inputs: %{
    "now" => DateTime.utc_now(),
    "now in Tahiti" => DateTime.shift_zone!(DateTime.utc_now(), "Pacific/Tahiti")
  }
)
