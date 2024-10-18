Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

default_test_db = System.get_env("CH_DATABASE", "ch_elixir_test")
Ch.HTTP.query!("DROP DATABASE IF EXISTS {db:Identifier}", %{"db" => default_test_db})
Ch.HTTP.query!("CREATE DATABASE {db:Identifier}}", %{"db" => default_test_db})
Application.put_env(:ch, :database, default_test_db)

ExUnit.start(exclude: [:slow])
