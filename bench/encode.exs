defmodule ClickhouseEventV2 do
  use Ecto.Schema

  @primary_key false
  schema "events_v2" do
    field(:name, Ch, type: "LowCardinality(String)")
    field(:site_id, Ch, type: "UInt64")
    field(:hostname, :string)
    field(:pathname, :string)
    field(:user_id, Ch, type: "UInt64")
    field(:session_id, Ch, type: "UInt64")
    field(:timestamp, :naive_datetime)

    field(:"meta.key", {:array, :string})
    field(:"meta.value", {:array, :string})

    field(:revenue_source_amount, Ch, type: "Nullable(Decimal64(3))")
    field(:revenue_source_currency, Ch, type: "FixedString(3)")
    field(:revenue_reporting_amount, Ch, type: "Nullable(Decimal64(3))")
    field(:revenue_reporting_currency, Ch, type: "FixedString(3)")

    # Session attributes
    field(:referrer, :string)
    field(:referrer_source, :string)
    field(:utm_medium, :string)
    field(:utm_source, :string)
    field(:utm_campaign, :string)
    field(:utm_content, :string)
    field(:utm_term, :string)

    field(:country_code, Ch, type: "FixedString(2)")
    field(:subdivision1_code, Ch, type: "LowCardinality(String)")
    field(:subdivision2_code, Ch, type: "LowCardinality(String)")
    field(:city_geoname_id, Ch, type: "UInt32")

    field(:screen_size, Ch, type: "LowCardinality(String)")
    field(:operating_system, Ch, type: "LowCardinality(String)")
    field(:operating_system_version, Ch, type: "LowCardinality(String)")
    field(:browser, Ch, type: "LowCardinality(String)")
    field(:browser_version, Ch, type: "LowCardinality(String)")
  end
end

defmodule Ecto.Adapters.ClickHouse.Schema do
  def remap_type(type, schema, field) do
    remap_type(Ecto.Type.type(type), type, schema, field)
  end

  defp remap_type({:parameterized, Ch, t}, _original, _schema, _field), do: t

  defp remap_type(t, _original, _schema, _field)
       when t in [:string, :date, :uuid, :boolean],
       do: t

  defp remap_type(dt, _original, _schema, _field)
       when dt in [:naive_datetime, :utc_datetime],
       do: :datetime

  defp remap_type(usec, _original, _schema, _field)
       when usec in [:naive_datetime_usec, :utc_datetime_usec],
       do: {:datetime64, _precision = 6}

  # TODO remove
  defp remap_type(t, _original, _schema, _field)
       when t in [:binary, :binary_id],
       do: :string

  # TODO remove
  for size <- [8, 16, 32, 64, 128, 256] do
    defp remap_type(unquote(:"u#{size}") = u, _original, _schema, _field), do: u
    defp remap_type(unquote(:"i#{size}") = i, _original, _schema, _field), do: i
  end

  defp remap_type({:array = a, t}, original, schema, field),
    do: {a, remap_type(t, original, schema, field)}
end

defmodule Bench do
  fields = ClickhouseEventV2.__schema__(:fields)

  types =
    Enum.map(fields, fn field ->
      type = ClickhouseEventV2.__schema__(:type, field) || raise "missing type for #{field}"

      type
      |> Ecto.Type.type()
      |> Ecto.Adapters.ClickHouse.Schema.remap_type(ClickhouseEventV2, field)
    end)

  encoding_types = Ch.RowBinary.encoding_types(types)

  def current_encoder(event) do
    [Enum.map(unquote(fields), fn field -> Map.fetch!(event, field) end)]
    |> Ch.RowBinary._encode_rows(unquote(encoding_types))
    |> IO.iodata_to_binary()
  end

  def next_encoder(%{
        name: name,
        site_id: site_id,
        hostname: hostname,
        pathname: pathname,
        user_id: user_id,
        session_id: session_id,
        timestamp: timestamp,
        "meta.key": meta_keys,
        "meta.value": meta_values,
        revenue_source_amount: revenue_source_amount,
        revenue_source_currency: revenue_source_currency,
        revenue_reporting_amount: revenue_reporting_amount,
        revenue_reporting_currency: revenue_reporting_currency,
        referrer: referrer,
        referrer_source: referrer_source,
        utm_medium: utm_medium,
        utm_source: utm_source,
        utm_campaign: utm_campaign,
        utm_content: utm_content,
        utm_term: utm_term,
        country_code: country_code,
        subdivision1_code: subdivision1_code,
        subdivision2_code: subdivision2_code,
        city_geoname_id: city_geoname_id,
        screen_size: screen_size,
        operating_system: operating_system,
        operating_system_version: operating_system_version,
        browser: browser,
        browser_version: browser_version
      }) do
    site_id = site_id || 0
    user_id = user_id || 0
    session_id = session_id || 0
    city_geoname_id = city_geoname_id || 0
    timestamp = if timestamp, do: to_unix(timestamp), else: 0

    [
      encode_string(name),
      <<site_id::64-unsigned>>,
      encode_string(hostname),
      encode_string(pathname),
      <<user_id::64-unsigned, session_id::64-unsigned, timestamp::16-unsigned>>,
      encode_string_array(meta_keys),
      encode_string_array(meta_values),
      encode_nullable(revenue_source_amount),
      encode_fixed_string_3(revenue_source_currency),
      encode_nullable(revenue_reporting_amount),
      encode_fixed_string_3(revenue_reporting_currency),
      encode_string(referrer),
      encode_string(referrer_source),
      encode_string(utm_medium),
      encode_string(utm_source),
      encode_string(utm_campaign),
      encode_string(utm_content),
      encode_string(utm_term),
      encode_fixed_string_2(country_code),
      encode_string(subdivision1_code),
      encode_string(subdivision2_code),
      <<city_geoname_id::32-unsigned>>,
      encode_string(screen_size),
      encode_string(operating_system),
      encode_string(operating_system_version),
      encode_string(browser),
      encode_string(browser_version)
    ]
  end

  # @compile inline: [encode_varint: 1]
  defp encode_varint(i) when i < 128, do: i
  defp encode_varint(i), do: encode_varint_cont(i)

  defp encode_varint_cont(i) when i < 128, do: <<i>>

  import Bitwise

  defp encode_varint_cont(i) do
    [(i &&& 0b0111_1111) ||| 0b1000_0000 | encode_varint_cont(i >>> 7)]
  end

  @compile inline: [encode_string: 1]
  defp encode_string(str) when is_binary(str) do
    [encode_varint(byte_size(str)) | str]
  end

  defp encode_string(nil), do: 0

  @compile inline: [encode_string_array: 1]
  def encode_string_array([]), do: 0
  # def encode_string_array([e1]), do: [1, encode_varint(byte_size(e1)) | e1]

  # def encode_string_array([e1, e2]) do
  #   [2, encode_varint(byte_size(e1)), e1, encode_varint(byte_size(e2)), e2]
  # end

  # def encode_string_array([e1, e2, e3]) do
  #   [
  #     2,
  #     encode_varint(byte_size(e1)),
  #     e1,
  #     encode_varint(byte_size(e2)),
  #     e2,
  #     encode_varint(byte_size(e3)),
  #     e3
  #   ]
  # end

  def encode_string_array([_ | _] = arr) do
    [encode_varint(length(arr)) | encode_strings(arr)]
  end

  def encode_string_array(nil), do: 0

  defp encode_strings([s | rest]), do: [encode_string(s) | encode_strings(rest)]
  defp encode_strings([]), do: []

  @compile inline: [encode_fixed_string_2: 1]
  defp encode_fixed_string_2(str) when byte_size(str) == 2, do: str
  defp encode_fixed_string_2(nil), do: <<0, 0>>

  @compile inline: [encode_fixed_string_3: 1]
  defp encode_fixed_string_3(str) when byte_size(str) == 3, do: str
  defp encode_fixed_string_3(nil), do: <<0, 0, 0>>

  @compile inline: [encode_nullable: 1]
  defp encode_nullable(nil), do: 1

  defp to_unix(%{year: year, month: month, day: day, hour: hour, minute: minute, second: second}) do
    to_unix(year, month, day, hour, minute, second)
  end

  @compile inline: [to_unix: 6]

  for year <- 2024..2025, month <- 1..12 do
    epoch = DateTime.to_unix(DateTime.new!(Date.new!(year, month, 1), Time.new!(0, 0, 0)))

    defp to_unix(unquote(year), unquote(month), day, hour, minute, second) do
      unquote(epoch) + (day - 1) * 86400 + hour * 3600 + minute * 60 + second
    end
  end

  def run do
    Benchee.run(
      %{
        "current encoder" => &Bench.current_encoder/1,
        "next encoder" => &Bench.next_encoder/1
      },
      # profile_after: true,
      inputs: %{
        # "empty" => %ClickhouseEventV2{},
        "pageview" => %ClickhouseEventV2{
          name: "pageview",
          site_id: 3,
          hostname: "stats.copycat.fun",
          pathname: "/",
          user_id: 18_167_959_095_776_540_841,
          session_id: 18_173_968_599_266_389_340,
          timestamp: ~N[2024-07-13 12:53:59],
          country_code: "TH",
          screen_size: "Mobile",
          operating_system: "iOS",
          operating_system_version: "17.5",
          browser: "Safari",
          browser_version: "17.5"
        }
      }
    )
  end
end

Bench.run()
