# Changelog

## Unreleased

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
