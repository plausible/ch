# DateTime and Time Zones

ClickHouse `DateTime` and `DateTime64` values are stored as Unix timestamps. A time zone affects how a value is parsed from text and how it is displayed, not the stored instant.

`Ch` has two relevant encoding paths:

- Query parameters are sent as text in the HTTP URL.
- RowBinary values are sent as binary Unix timestamps.

## Query Parameters

For query parameters, `Ch` encodes:

- `NaiveDateTime` as an ISO8601 datetime without a time zone, for example `2022-12-12T12:00:00`.
- `DateTime` as a Unix timestamp shifted to UTC, for example `1670846400`.

ClickHouse then interprets the parameter according to the query parameter type.

| Elixir value | ClickHouse parameter type | Server/session time zone | Meaning |
| --- | --- | --- | --- |
| `~N[2022-12-12 12:00:00]` | `DateTime` | server UTC | parsed as noon UTC |
| `~N[2022-12-12 12:00:00]` | `DateTime` | server `Europe/Berlin` | parsed as noon Berlin time |
| `~N[2022-12-12 12:00:00]` | `DateTime` | `session_timezone: "Asia/Bangkok"` | parsed as noon Bangkok time |
| `~N[2022-12-12 12:00:00]` | `DateTime('UTC')` | any | parsed as noon UTC |
| `~N[2022-12-12 12:00:00]` | `DateTime('Asia/Bangkok')` | any | parsed as noon Bangkok time |
| `~U[2022-12-12 12:00:00Z]` | `DateTime` | any | sent as a UTC Unix timestamp |
| `DateTime` in any zone | `DateTime('Asia/Bangkok')` | any | sent as a UTC Unix timestamp, displayed in Bangkok time |

The same rules apply to `DateTime64`, except fractional precision is preserved.

## Session Time Zone

ClickHouse's `session_timezone` setting controls the implicit time zone for `DateTime` and `DateTime64` types that do not specify one.

```elixir
Ch.query!(
  pool,
  "SELECT {dt:DateTime} AS d, toString(d), timeZone()",
  %{"dt" => ~N[2022-12-12 12:00:00]},
  settings: [session_timezone: "Asia/Bangkok"]
)
```

returns the stored UTC instant for noon in Bangkok, while `toString(d)` displays the session-local value:

```elixir
%{rows: [[~N[2022-12-12 05:00:00], "2022-12-12 12:00:00", "Asia/Bangkok"]]}
```

An explicit ClickHouse type time zone ignores `session_timezone`:

```elixir
Ch.query!(
  pool,
  "SELECT {dt:DateTime('Asia/Bangkok')} AS d, toString(d)",
  %{"dt" => ~N[2022-12-12 12:00:00]},
  settings: [session_timezone: "UTC"]
)
```

returns:

```elixir
%{rows: [[#DateTime<2022-12-12 12:00:00+07:00 +07 Asia/Bangkok>, "2022-12-12 12:00:00"]]}
```

## Decoding Results

When Ch decodes `RowBinaryWithNamesAndTypes`:

| ClickHouse result type | Elixir value |
| --- | --- |
| `DateTime` | `NaiveDateTime` in UTC |
| `DateTime64(P)` | `NaiveDateTime` in UTC |
| `DateTime('UTC')` | `DateTime` in UTC |
| `DateTime64(P, 'UTC')` | `DateTime` in UTC |
| `DateTime('Europe/Berlin')` | `DateTime` in `Europe/Berlin` |
| `DateTime64(P, 'Europe/Berlin')` | `DateTime` in `Europe/Berlin` |

For implicit time zone result types, ClickHouse sends only the stored Unix timestamp and the type name `DateTime` or `DateTime64(P)`. The response does not include the server or session time zone, so Ch decodes those values as UTC `NaiveDateTime`.

For explicit time zone result types, the time zone is part of the type name, so Ch decodes to `DateTime` in that zone.

## RowBinary Inserts

RowBinary does not send text for `DateTime` values. It sends Unix timestamps directly.

| Elixir value | RowBinary type | Encoding |
| --- | --- | --- |
| `NaiveDateTime` | `DateTime` | treated as a UTC naive value and encoded as Unix seconds |
| `NaiveDateTime` | `DateTime64(P)` | treated as a UTC naive value and encoded as Unix ticks |
| `DateTime` | `DateTime` | encoded as Unix seconds for the instant |
| `DateTime` | `DateTime64(P)` | encoded as Unix ticks for the instant |
| `NaiveDateTime` | `DateTime('Europe/Berlin')` | treated as Berlin wall time and encoded as Unix seconds |
| `NaiveDateTime` | `DateTime64(P, 'Europe/Berlin')` | treated as Berlin wall time and encoded as Unix ticks |
| `DateTime` | `DateTime('Europe/Berlin')` | encoded as Unix seconds for the instant |
| `DateTime` | `DateTime64(P, 'Europe/Berlin')` | encoded as Unix ticks for the instant |

For timezone-qualified `DateTime` and `DateTime64` types, `NaiveDateTime` values are interpreted in the timezone from the ClickHouse type. `DateTime` values already represent an instant, so their own timezone is normalized to Unix seconds or ticks before encoding.

## Practical Guidance

- Prefer `DateTime` values when the Elixir value represents a real instant.
- Use `NaiveDateTime` only when you intentionally want ClickHouse to interpret the wall time using an implicit or explicit ClickHouse time zone.
- Prefer explicit ClickHouse column types like `DateTime('UTC')` or `DateTime64(6, 'UTC')` for unambiguous schemas.
- Use `session_timezone` in tests when you need deterministic behavior for implicit `DateTime` or `DateTime64` types without changing the ClickHouse server time zone.
