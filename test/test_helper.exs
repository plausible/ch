Application.put_env(:elixir, :time_zone_database, Ch.Tzdb)
# TODO create / drop non-default db
ExUnit.start()
