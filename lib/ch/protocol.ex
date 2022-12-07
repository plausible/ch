defmodule Ch.Protocol do
  @moduledoc false
  import Bitwise

  def encode_row([el | els], [type | types]), do: [encode(type, el) | encode_row(els, types)]
  def encode_row([] = done, []), do: done

  def encode_rows([row | rows], types), do: [encode_row(row, types) | encode_rows(rows, types)]
  def encode_rows([] = done, _types), do: done

  # TODO
  def encode(:varint, num) when num < 128, do: <<num>>
  def encode(:varint, num), do: <<1::1, num::7, encode(:varint, num >>> 7)::bytes>>

  def encode(:string, str) do
    [encode(:varint, byte_size(str)) | str]
  end

  def encode(:u8, i), do: <<i::little>>
  def encode(:u16, i), do: <<i::16-little>>
  def encode(:u32, i), do: <<i::32-little>>
  def encode(:u64, i), do: <<i::64-little>>

  def encode(:i8, i), do: <<i::little-signed>>
  def encode(:i16, i), do: <<i::16-little-signed>>
  def encode(:i32, i), do: <<i::32-little-signed>>
  def encode(:i64, i), do: <<i::64-little-signed>>

  def encode(:f64, f), do: <<f::64-little-signed-float>>
  def encode(:f32, f), do: <<f::32-little-signed-float>>

  def encode(:boolean, true), do: encode(:u8, 1)
  def encode(:boolean, false), do: encode(:u8, 0)

  @epoch_date ~D[1970-01-01]
  @epoch_naive_datetime NaiveDateTime.new!(@epoch_date, ~T[00:00:00])

  def encode(:datetime, %NaiveDateTime{} = datetime) do
    <<NaiveDateTime.diff(datetime, @epoch_naive_datetime)::32-little>>
  end

  def encode(:date, %Date{} = date) do
    <<Date.diff(date, @epoch_date)::16-little>>
  end
end
