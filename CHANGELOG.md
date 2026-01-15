# Changelog

## 0.7.1 (2026-01-15)

- fix negative integer parsing in Enum8 and Enum16 types https://github.com/plausible/ch/pull/295

## 0.7.0 (2026-01-13)

- use `disconnect_and_retry` (added in DBConnection v2.9.0) instead of `disconnect` for connection errors https://github.com/plausible/ch/pull/292

## 0.6.2 (2026-01-03)

- added support for `multipart/form-data` in queries: https://github.com/plausible/ch/pull/290 -- which allows bypassing URL length limits sometimes imposed by reverse proxies when sending queries with many parameters.
  
  ⚠️ This is currently **opt-in** per query ⚠️
  
  Global support for the entire connection pool is planned for a future release.

  **Usage**
  
  Pass `multipart: true` in the options list for `Ch.query/4`

  ```elixir
  # Example usage
  Ch.query(pool, "SELECT {a:String}, {b:String}", %{"a" => "A", "b" => "B"}, multipart: true)
  ```

  <details>
  <summary>View raw request format reference</summary>

  ```http
  POST / HTTP/1.1
  content-length: 387
  host: localhost:8123
  user-agent: ch/0.6.2-dev
  x-clickhouse-format: RowBinaryWithNamesAndTypes
  content-type: multipart/form-data; boundary="ChFormBoundaryZZlfchKTcd8ToWjEvn66i3lAxNJ_T9dw"

  --ChFormBoundaryZZlfchKTcd8ToWjEvn66i3lAxNJ_T9dw
  content-disposition: form-data; name="param_a"

  A
  --ChFormBoundaryZZlfchKTcd8ToWjEvn66i3lAxNJ_T9dw
  content-disposition: form-data; name="param_b"

  B
  --ChFormBoundaryZZlfchKTcd8ToWjEvn66i3lAxNJ_T9dw
  content-disposition: form-data; name="query"

  select {a:String}, {b:String}
  --ChFormBoundaryZZlfchKTcd8ToWjEvn66i3lAxNJ_T9dw--
  ```

  </details>

## 0.6.1 (2025-12-04)

- handle disconnect during stream https://github.com/plausible/ch/pull/283

## 0.6.0 (2025-11-26)

- added **automatic decoding** to `Ch.stream/4` when using `RowBinaryWithNamesAndTypes` format: https://github.com/plausible/ch/pull/277.

    Previously, this function returned raw bytes.
  
    To **restore the previous behavior** (raw bytes/no automatic decoding), pass `decode: false` in the options (**fourth** argument).

    **Example of required change to preserve the previous behavior**

    ```elixir
    # before, no decoding by default
    DBConnection.run(pool, fn conn ->
      conn
      |> Ch.stream("select number from numbers(10)")
      |> Enum.into([])
    end)

    # after, to keep the same behaviour add `decode: false` option
    DBConnection.run(pool, fn conn ->
      conn
      |> Ch.stream("select number from numbers(10)", _params = %{}, decode: false)
      |> Enum.into([])
    end)
  ```

  Queries using other explicit formats like `CSVWithNames` are **unaffected** and can remain as they are.

  **Examples of unaffected queries**

  ```elixir
  DBConnection.run(pool, fn conn ->
    conn
    |> Ch.stream("select number from numbers(10) format CSVWithNames")
    |> Enum.into([])
  end)

  DBConnection.run(pool, fn conn ->
    conn
    |> Ch.stream("select number from numbers(10)", _params = %{}, format: "CSVWithNames")
    |> Enum.into([])
  end)
  ```

## 0.5.7 (2025-11-26)

- fix type decoding for strings containing newlines https://github.com/plausible/ch/pull/278

## 0.5.6 (2025-08-26)

- fix internal type ordering in Variant https://github.com/plausible/ch/pull/275

## 0.5.5 (2025-08-26)

- fix version check for adding JSON settings https://github.com/plausible/ch/pull/274

## 0.5.4 (2025-07-22)

- allow `nil` in params https://github.com/plausible/ch/pull/268

## 0.5.2 (2025-07-21)

- make Dynamic usable in Ecto schemas https://github.com/plausible/ch/pull/267

## 0.5.1 (2025-07-20)

- add partial [Dynamic](https://clickhouse.com/docs/sql-reference/data-types/dynamic) type support https://github.com/plausible/ch/pull/266

## 0.5.0 (2025-07-17)

- add [Time](https://clickhouse.com/docs/sql-reference/data-types/time) and [Time64](https://clickhouse.com/docs/sql-reference/data-types/time64) types support https://github.com/plausible/ch/pull/260
- add [Variant](https://clickhouse.com/docs/sql-reference/data-types/variant) type support https://github.com/plausible/ch/pull/263
- add [JSON](https://clickhouse.com/docs/sql-reference/data-types/newjson) type support https://github.com/plausible/ch/pull/262

## 0.4.1 (2025-07-07)

- fix column decoding when count exceeds 127 https://github.com/plausible/ch/pull/257

## 0.4.0 (2025-06-19)

- restrict to Ecto v3.13

## 0.3.4 (2025-07-07)

- fix column decoding when count exceeds 127 https://github.com/plausible/ch/pull/257

## 0.3.3 (2025-06-19)

- restrict to Ecto v3.12

## 0.3.2 (2025-02-25)

- fix type decoding when type name exceeds 127 bytes https://github.com/plausible/ch/pull/248

## 0.3.1 (2025-02-08)

- add column names to `%Ch.Result{}` https://github.com/plausible/ch/pull/243

## 0.3.0 (2025-02-03)

- gracefully handle `connection: closed` response from server https://github.com/plausible/ch/pull/211
- allow non-UTC `DateTime.t()` in query params https://github.com/plausible/ch/pull/223
- allow non-UTC `DateTime.t()` when encoding RowBinary https://github.com/plausible/ch/pull/225
- add `:types` to `query_option` typespec https://github.com/plausible/ch/pull/234
- handle missing `written_rows` in insert https://github.com/plausible/ch/pull/236

## 0.2.10 (2025-02-03)

- handle missing `written_rows` in insert https://github.com/plausible/ch/pull/236 (backported)

## 0.2.9 (2024-11-04)

- catch all errors in `connect/1` to avoid triggering Supervisor https://github.com/plausible/ch/pull/209

## 0.2.8 (2024-09-06)

- support named tuples https://github.com/plausible/ch/pull/197

## 0.2.7 (2024-08-15)

- raise on invalid UInt8 and Int8 when encoding RowBinary https://github.com/plausible/ch/pull/180
- adapt to Ecto v3.12 https://github.com/plausible/ch/pull/195

## 0.2.6 (2024-05-30)

- fix query encoding for datetimes where the microseconds value starts with zeroes `~U[****-**-** **:**:**.0*****]` https://github.com/plausible/ch/pull/175

## 0.2.5 (2024-03-05)

- add `:data` in `%Ch.Result{}` https://github.com/plausible/ch/pull/159
- duplicate `Ch.Result.data` in `Ch.Result.rows` for backwards compatibility https://github.com/plausible/ch/pull/160
- make `Ch.stream` emit `Ch.Result.t` instead of `Mint.Types.response` https://github.com/plausible/ch/pull/161
- make `Ch.stream` collectable https://github.com/plausible/ch/pull/162

## 0.2.4 (2024-01-29)

- use `ch-#{version}` as user-agent https://github.com/plausible/ch/pull/154
- fix query string escaping for `\t`, `\\`, and `\n` https://github.com/plausible/ch/pull/155

## 0.2.3 (2024-01-29)

- fix socket leak on failed handshake https://github.com/plausible/ch/pull/153

## 0.2.2 (2023-12-23)

- fix query encoding for datetimes with zeroed microseconds `~U[****-**-** **:**:**.000000]` https://github.com/plausible/ch/pull/138

## 0.2.1 (2023-08-22)

- fix array casts with `Ch` subtype https://github.com/plausible/ch/pull/118

## 0.2.0 (2023-07-28)

- move loading and dumping from `Ch` type to the adapter https://github.com/plausible/ch/pull/112

## 0.1.14 (2023-05-24)

- simplify types, again...

## 0.1.13 (2023-05-24)

- refactor types in `Ch.RowBinary` https://github.com/plausible/ch/pull/88

## 0.1.12 (2023-05-24)

- replace `{:raw, data}` with `encode: false` option, add `:decode` option https://github.com/plausible/ch/pull/42

## 0.1.11 (2023-05-19)

- improve Enum error message invalid values during encoding: https://github.com/plausible/ch/pull/85
- fix `\t` and `\n` in query params https://github.com/plausible/ch/pull/86

## 0.1.10 (2023-05-05)

- support `:raw` option in `Ch` type https://github.com/plausible/ch/pull/84

## 0.1.9 (2023-05-02)

- relax deps versions

## 0.1.8 (2023-05-01)

- fix varint encoding

## 0.1.7 (2023-04-24)

- support RowBinaryWithNamesAndTypes

## 0.1.6 (2023-04-24)

- add Map(K,V) support in Ch Ecto type

## 0.1.5 (2023-04-23)

- fix query param encoding like Array(Date)
- add more types support in Ch Ecto type: tuples, ipv4, ipv6, geo

## 0.1.4 (2023-04-23)

- actually support negative `Enum` values

## 0.1.3 (2023-04-23)

- support negative `Enum` values, fix `Enum16` encoding

## 0.1.2 (2023-04-23)

- support `Enum8` and `Enum16` encoding

## 0.1.1 (2023-04-23)

- cleanup published docs
