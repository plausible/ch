Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

default_database = System.get_env("CH_DATABASE", "ch_elixir_test")
default_username = System.get_env("CH_USERNAME", "default")
default_password = System.get_env("CH_PASSWORD", "default")

Application.put_env(:ch, :default,
  database: default_database,
  username: default_username,
  password: default_password
)

Ch.Test.sql_exec("DROP DATABASE IF EXISTS {db:Identifier}", %{"db" => default_database},
  database: "default"
)

Ch.Test.sql_exec("CREATE DATABASE {db:Identifier}", %{"db" => default_database},
  database: "default"
)

ExUnit.start(exclude: [:slow])
