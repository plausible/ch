defmodule Ch.Error do
  @moduledoc "Error struct wrapping ClickHouse error responses."
  defexception [:code, :message]
  @type t :: %__MODULE__{code: pos_integer | nil, message: String.t()}

  def exception(code, message) do
    message = IO.iodata_to_binary(ensure_printable(message))
    exception(code: code, message: message)
  end

  @dialyzer :no_improper_lists

  @doc false
  def ensure_printable(message) do
    printable(message, 0, 0, message, [])
  end

  defguardp is_printable(char) when char in 32..127 or char == ?\n

  defp printable(<<char, rest::bytes>>, from, len, original, acc) when is_printable(char) do
    printable(rest, from, len + 1, original, acc)
  end

  defp printable(<<_char, rest::bytes>>, from, len, original, acc) do
    acc = [acc | binary_part(original, from, len)]
    unprintable(rest, from + len, 1, original, acc)
  end

  defp printable(<<>>, from, len, original, acc) do
    [acc | binary_part(original, from, len)]
  end

  defp unprintable(<<char, rest::bytes>>, from, len, original, acc) when is_printable(char) do
    acc = [acc | inspect(binary_part(original, from, len))]
    printable(rest, from + len, 1, original, acc)
  end

  defp unprintable(<<_char, rest::bytes>>, from, len, original, acc) do
    unprintable(rest, from, len + 1, original, acc)
  end

  defp unprintable(<<>>, from, len, original, acc) do
    [acc | inspect(binary_part(original, from, len))]
  end
end
