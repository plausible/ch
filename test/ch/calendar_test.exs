defmodule Ch.CalendarTest do
  use ExUnit.Case, parameterize: [%{query_options: []}, %{query_options: [multipart: true]}]

  import Ch.Test, only: [parameterize_query!: 3]

  setup ctx do
    if ctx[:async] do
      raise "Ch.CalendarTest cannot be async because it changes the VM-global timezone database"
    end

    :ok
  end

  test "timezone-qualified types require an Elixir timezone database", ctx do
    conn =
      start_supervised!(
        {Ch,
         database: Ch.Test.database(),
         pool_size: 1,
         settings: [session_timezone: "America/Los_Angeles"]}
      )

    ctx = Map.put(ctx, :conn, conn)
    previous_time_zone_database = Calendar.get_time_zone_database()
    Calendar.put_time_zone_database(Calendar.UTCOnlyTimeZoneDatabase)
    on_exit(fn -> Calendar.put_time_zone_database(previous_time_zone_database) end)

    assert_raise ArgumentError, ~r/:utc_only_time_zone_database/, fn ->
      parameterize_query!(ctx, "SELECT {$0:DateTime('Asia/Tokyo')}", [
        ~N[2022-12-12 12:00:00]
      ])
    end
  end
end
