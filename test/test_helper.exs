Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

default_test_db = System.get_env("CH_DATABASE", "ch_elixir_test")
{:ok, _} = Ch.Test.sql_exec("DROP DATABASE IF EXISTS #{default_test_db}")
{:ok, _} = Ch.Test.sql_exec("CREATE DATABASE #{default_test_db}")
Application.put_env(:ch, :database, default_test_db)

ExUnit.start(exclude: [:slow])
