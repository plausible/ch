defmodule Ch.Encode.Multipart do
  @moduledoc false

  alias Ch.Encode.Parameters

  @doc """
  Encodes a query statement and params into a multipart request.
  """
  @spec encode(iodata, map, [Ch.query_option()]) ::
          {list, Mint.Types.headers(), iodata}
  def encode(statement, params, opts) do
    types = Keyword.get(opts, :types)
    settings = Keyword.get(opts, :settings, [])
    default_format = if types, do: "RowBinary", else: "RowBinaryWithNamesAndTypes"
    format = Keyword.get(opts, :format) || default_format

    boundary = "ChFormBoundary" <> Base.url_encode64(:crypto.strong_rand_bytes(24))
    content_type = "multipart/form-data; boundary=\"#{boundary}\""
    enc_boundary = "--#{boundary}\r\n"

    multipart =
      params
      |> multipart_params(enc_boundary)
      |> add_multipart_part("query", statement, enc_boundary)
      |> then(&[&1 | "--#{boundary}--\r\n"])

    headers = [{"x-clickhouse-format", format}, {"content-type", content_type} | headers(opts)]

    {settings, headers, multipart}
  end

  defp multipart_params(params, boundary) when is_map(params) do
    multipart_named_params(Map.to_list(params), boundary, [])
  end

  defp multipart_params(params, boundary) when is_list(params) do
    multipart_positional_params(params, 0, boundary, [])
  end

  defp multipart_named_params([{name, value} | params], boundary, acc) do
    acc =
      add_multipart_part(
        acc,
        "param_" <> URI.encode_www_form(name),
        Parameters.encode(value),
        boundary
      )

    multipart_named_params(params, boundary, acc)
  end

  defp multipart_named_params([], _boundary, acc), do: acc

  defp multipart_positional_params([value | params], idx, boundary, acc) do
    acc =
      add_multipart_part(
        acc,
        "param_$" <> Integer.to_string(idx),
        Parameters.encode(value),
        boundary
      )

    multipart_positional_params(params, idx + 1, boundary, acc)
  end

  defp multipart_positional_params([], _idx, _boundary, acc), do: acc

  @compile inline: [add_multipart_part: 4]
  defp add_multipart_part(multipart, name, value, boundary) do
    part = [
      boundary,
      "content-disposition: form-data; name=\"",
      name,
      "\"\r\n\r\n",
      value,
      "\r\n"
    ]

    case multipart do
      [] -> part
      _ -> [multipart | part]
    end
  end

  @spec headers(Keyword.t()) :: Mint.Types.headers()
  defp headers(opts), do: Keyword.get(opts, :headers, [])
end
