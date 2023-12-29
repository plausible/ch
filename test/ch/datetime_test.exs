# Note on datetime encoding in query parameters:

# - `%NaiveDateTime{}` is encoded as text to make it assume the column's or ClickHouse's timezone

# ```elixir
# Mix.install([:ch, :tz])

# {:ok, pid} = Ch.start_link()
# naive = ~N[2023-12-16 12:00:00]

# %Ch.Result{rows: [["UTC"]]} = Ch.query!(pid, "SELECT timezone()")

# %Ch.Result{rows: [[~N[2023-12-16 12:00:00]]]} =
#   Ch.query!(pid, "SELECT {naive:DateTime}", %{"naive" => naive})

# # https://clickhouse.com/docs/en/operations/settings/settings#session_timezone
# %Ch.Result{rows: [["Europe/Berlin"]]} =
#   Ch.query!(pid, "SELECT timezone()", [], settings: [session_timezone: "Europe/Berlin"])

# %Ch.Result{rows: [[~N[2023-12-16 11:00:00]]]} =
#   Ch.query!(pid, "SELECT {naive:DateTime}", %{"naive" => naive}, settings: [session_timezone: "Europe/Berlin"])

# :ok = Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

# %Ch.Result{rows: [[taipei]]} =
#   Ch.query!(pid, "SELECT {naive:DateTime('Asia/Taipei')}", %{"naive" => naive})

# "#DateTime<2023-12-16 12:00:00+08:00 CST Asia/Taipei>" = inspect(taipei)
# ```

# - `%DateTime{time_zone: "Etc/UTC"}` is encoded as unix timestamp and is treated as UTC timestamp by ClickHouse

# ```elixir
# {:ok, pid} = Ch.start_link()

# ```

# - encoding non-UTC `%DateTime{}` requires a timezone database be configured

# ```elixir
# Mix.install([:ch, :tz])

# :ok = Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

# utc = ~U[2023-12-16 10:20:51Z]
# taipei = DateTime.new!(~D[2023-12-16], ~T[18:20:51], "Asia/Taipei")
# berlin = DateTime.new!(~D[2023-12-16], ~T[11:20:51], "Europe/Berlin")

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {utc:DateTime}", %{"utc" => utc})

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {taipei:DateTime}", %{"taipei" => taipei})

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {berlin:DateTime}", %{"berlin" => berlin})

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {utc:DateTime}", %{"utc" => utc}, settings: [session_timezone: "Europe/Berlin"])

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {taipei:DateTime('UTC')}", %{"ts" => taipei})

# %Ch.Result{rows: []} =
#   Ch.query!(pid, "SELECT {taipei:DateTime('Asia/Taipei')}", %{"ts" => taipei})

# ```
# Mix.install([:ch, :tz])

# :ok = Calendar.put_time_zone_database(Tz.TimeZoneDatabase)

# {:ok, pid} = Ch.start_link()

# %Ch.Result{rows: [[~N[2023-04-25 17:45:09]]]} =
#   Ch.query!(pid, "SELECT CAST(now() as DateTime)")

# %Ch.Result{rows: [[~U[2023-04-25 17:45:11Z]]]} =
#   Ch.query!(pid, "SELECT CAST(now() as DateTime('UTC'))")

# %Ch.Result{rows: [[%DateTime{time_zone: "Asia/Taipei"} = taipei]]} =
#   Ch.query!(pid, "SELECT CAST(now() as DateTime('Asia/Taipei'))")

# "2023-04-26 01:45:12+08:00 CST Asia/Taipei" = to_string(taipei)
