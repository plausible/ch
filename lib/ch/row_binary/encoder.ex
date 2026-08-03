defmodule Ch.RowBinary.Encoder do
  @moduledoc """
  Compile-time RowBinary encoders for fixed, performance-sensitive schemas.

  Generated encoders inline common type encodings and fall back to
  `Ch.RowBinary.encode/2` for uncommon types. Each schema duplicates encoder
  code in the caller, so use this for hot paths and prefer
  `Ch.RowBinary.prepare/1` when compile-time specialization is unnecessary.

  Schema order is significant. List schemas preserve their declared order;
  map schemas are sorted by their column names to make their otherwise
  unspecified order deterministic. That order controls map field encoding,
  list-row positions, and the names/types insert header.

  By default, generated functions accept atom- or string-keyed maps (including
  structs) according to the schema keys:

      defmodule Events do
        require Ch.RowBinary.Encoder

        Ch.RowBinary.Encoder.define_encoder(
          name: :encode_rows,
          schema: [id: "UInt64", name: "String"],
          rows: true
        )
      end

  Set `row: :list` to accept row lists in schema order. Supplying `table:`
  implies `rows: true` and prepends a complete
  `INSERT ... FORMAT RowBinaryWithNamesAndTypes` header. Table identifiers may
  contain dot-separated unquoted ClickHouse identifier components only.
  """

  import Bitwise

  @epoch_gregorian_seconds 62_167_219_200
  @epoch_gregorian_days 719_528
  @identifier ~r/^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/

  # Keeps the compiler from carrying the outer non-nil case refinement into a
  # nested specialized expression whose standalone form also handles nil.
  @doc false
  def non_nil(value), do: value

  defmacro define_encoder(opts_ast) do
    opts = compile_time_value!(opts_ast, __CALLER__, :options)
    validate_options!(opts)
    build_encoder_definition(opts)
  end

  defmacro define_encoder(name_ast, opts_ast) do
    name = compile_time_value!(name_ast, __CALLER__, :name)
    opts = compile_time_value!(opts_ast, __CALLER__, :options)
    validate_options!(opts)

    unless is_atom(name) do
      raise ArgumentError, "expected encoder name to be an atom, got: #{inspect(name)}"
    end

    opts =
      case Keyword.fetch(opts, :name) do
        {:ok, ^name} ->
          opts

        {:ok, other} ->
          raise ArgumentError, "conflicting encoder names: #{inspect(name)} and #{inspect(other)}"

        :error ->
          Keyword.put(opts, :name, name)
      end

    build_encoder_definition(opts)
  end

  defp compile_time_value!(ast, caller, label) do
    {value, _binding} = Code.eval_quoted(ast, [], caller)
    value
  rescue
    _error ->
      reraise ArgumentError,
              [
                message:
                  "expected define_encoder #{label} to be available at compile time, got: #{Macro.to_string(ast)}"
              ],
              __STACKTRACE__
  end

  defp validate_options!(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError, "expected define_encoder options to be a keyword list"
    end

    unknown = Keyword.keys(opts) -- [:name, :schema, :rows, :row, :table]

    if unknown != [] do
      raise ArgumentError, "unknown define_encoder options: #{inspect(unknown)}"
    end
  end

  defp build_encoder_definition(opts) do
    name = Keyword.fetch!(opts, :name)
    schema = Keyword.fetch!(opts, :schema)
    row_format = Keyword.get(opts, :row, :map)
    table = Keyword.get(opts, :table)
    rows? = Keyword.get(opts, :rows, not is_nil(table))

    unless is_atom(name) do
      raise ArgumentError, "expected encoder name to be an atom, got: #{inspect(name)}"
    end

    unless row_format in [:map, :list] do
      raise ArgumentError, "expected :row to be :map or :list, got: #{inspect(row_format)}"
    end

    unless is_boolean(rows?) do
      raise ArgumentError, "expected :rows to be a boolean, got: #{inspect(rows?)}"
    end

    if table && not rows? do
      raise ArgumentError, ":table requires :rows to be true"
    end

    columns = encoder_columns!(schema)
    {pattern, row_iodata} = encoder_row(columns, row_format)

    cond do
      table -> build_insert(name, pattern, row_iodata, table, columns)
      rows? -> build_many(name, pattern, row_iodata)
      true -> build_one(name, pattern, row_iodata)
    end
  end

  defp build_insert(name, pattern, row_iodata, table, columns) do
    rows_fun = private_name(name)
    prefix = insert_prefix!(table, columns)

    quote do
      def unquote(name)(rows), do: [unquote(prefix) | unquote(rows_fun)(rows)]

      defp unquote(rows_fun)([unquote(pattern) | rows]) do
        [unquote(row_iodata) | unquote(rows_fun)(rows)]
      end

      defp unquote(rows_fun)([]), do: []
    end
  end

  defp build_many(name, pattern, row_iodata) do
    rows_fun = private_name(name)

    quote do
      def unquote(name)(rows), do: unquote(rows_fun)(rows)

      defp unquote(rows_fun)([unquote(pattern) | rows]) do
        [unquote(row_iodata) | unquote(rows_fun)(rows)]
      end

      defp unquote(rows_fun)([]), do: []
    end
  end

  defp build_one(name, pattern, row_iodata) do
    quote do
      def unquote(name)(unquote(pattern)), do: unquote(row_iodata)
    end
  end

  defp private_name(name), do: :"__ch_rowbinary_#{name}_rows__"

  defp insert_prefix!(table, columns) when is_atom(table) do
    insert_prefix!(Atom.to_string(table), columns)
  end

  defp insert_prefix!(table, columns) when is_binary(table) do
    unless Regex.match?(@identifier, table) do
      raise ArgumentError,
            "invalid ClickHouse table identifier: #{inspect(table)}; expected dot-separated unquoted identifiers"
    end

    names = Enum.map(columns, &elem(&1, 1))
    types = Enum.map(columns, &elem(&1, 2))

    IO.iodata_to_binary([
      "INSERT INTO ",
      table,
      " FORMAT RowBinaryWithNamesAndTypes\n",
      Ch.RowBinary.encode_names_and_types(names, types)
    ])
  end

  defp insert_prefix!(table, _columns) do
    raise ArgumentError, "expected :table to be a string or atom, got: #{inspect(table)}"
  end

  defp encoder_columns!(schema) when is_map(schema) do
    schema
    |> Map.to_list()
    |> Enum.sort_by(fn {key, _type} -> column_name!(key) end)
    |> encoder_columns!()
  end

  defp encoder_columns!(schema) when is_list(schema) do
    Enum.with_index(schema, fn
      {key, type}, index ->
        column_name = column_name!(key)
        encoding_type = type |> List.wrap() |> Ch.RowBinary.encoding_types() |> hd()
        {key, column_name, type, encoding_type, Macro.var(:"column_#{index}", nil)}

      other, _index ->
        raise ArgumentError,
              "expected schema entries to be {name, type} pairs, got: #{inspect(other)}"
    end)
  end

  defp encoder_columns!(schema) do
    raise ArgumentError, "expected :schema to be a map or list, got: #{inspect(schema)}"
  end

  defp column_name!(key) when is_binary(key), do: key
  defp column_name!(key) when is_atom(key), do: Atom.to_string(key)

  defp column_name!(key) do
    raise ArgumentError, "expected schema keys to be strings or atoms, got: #{inspect(key)}"
  end

  defp encoder_row(columns, :map) do
    pattern =
      {:%{}, [], Enum.map(columns, fn {key, _, _, _, variable} -> {key, variable} end)}

    {pattern, row_iodata(columns)}
  end

  defp encoder_row(columns, :list) do
    pattern = Enum.map(columns, &elem(&1, 4))
    {pattern, row_iodata(columns)}
  end

  defp row_iodata(columns) do
    Enum.map(columns, fn {_, _, _, type, variable} -> encoder_expr(type, variable) end)
  end

  defp encoder_expr(:string, value) do
    quote do
      case unquote(value) do
        string when is_binary(string) ->
          [Ch.RowBinary.encode_varint(byte_size(string)) | string]

        iodata when is_list(iodata) ->
          [Ch.RowBinary.encode_varint(IO.iodata_length(iodata)) | iodata]

        nil ->
          0
      end
    end
  end

  defp encoder_expr(:boolean, value) do
    quote do
      case unquote(value) do
        true -> 1
        false -> 0
        nil -> 0
      end
    end
  end

  defp encoder_expr(:u8, value) do
    quote do
      case unquote(value) do
        integer when is_integer(integer) and integer >= 0 and integer <= 255 -> integer
        nil -> 0
        term -> raise ArgumentError, "invalid UInt8: #{inspect(term)}"
      end
    end
  end

  defp encoder_expr(:i8, value) do
    quote do
      case unquote(value) do
        integer when is_integer(integer) and integer >= 0 and integer <= 127 ->
          integer

        integer when is_integer(integer) and integer < 0 and integer >= -128 ->
          <<integer::signed>>

        nil ->
          0

        term ->
          raise ArgumentError, "invalid Int8: #{inspect(term)}"
      end
    end
  end

  for size <- [16, 32, 64, 128, 256] do
    unsigned = :"u#{size}"
    signed = :"i#{size}"
    unsigned_max = (1 <<< size) - 1
    signed_min = -(1 <<< (size - 1))
    signed_max = (1 <<< (size - 1)) - 1

    defp encoder_expr(unquote(unsigned), value) do
      size = unquote(size)
      unsigned_max = unquote(unsigned_max)

      quote do
        case unquote(value) do
          integer
          when is_integer(integer) and integer >= 0 and integer <= unquote(unsigned_max) ->
            <<integer::unquote(size)-little>>

          nil ->
            <<0::unquote(size)>>

          term ->
            raise ArgumentError, "invalid UInt#{unquote(size)}: #{inspect(term)}"
        end
      end
    end

    defp encoder_expr(unquote(signed), value) do
      size = unquote(size)
      signed_min = unquote(signed_min)
      signed_max = unquote(signed_max)

      quote do
        case unquote(value) do
          integer
          when is_integer(integer) and integer >= unquote(signed_min) and
                 integer <= unquote(signed_max) ->
            <<integer::unquote(size)-little-signed>>

          nil ->
            <<0::unquote(size)>>

          term ->
            raise ArgumentError, "invalid Int#{unquote(size)}: #{inspect(term)}"
        end
      end
    end
  end

  for size <- [32, 64] do
    type = :"f#{size}"

    defp encoder_expr(unquote(type), value) do
      size = unquote(size)

      quote do
        case unquote(value) do
          number when is_number(number) -> <<number::unquote(size)-little-signed-float>>
          nil -> <<0::unquote(size)>>
        end
      end
    end
  end

  defp encoder_expr({:fixed_string, size} = type, value) do
    quote do
      case unquote(value) do
        string when is_binary(string) and byte_size(string) == unquote(size) ->
          string

        string when is_binary(string) and byte_size(string) < unquote(size) ->
          [string | <<0::size((unquote(size) - byte_size(string)) * 8)>>]

        nil ->
          <<0::size(unquote(size) * 8)>>

        term ->
          Ch.RowBinary.encode(unquote(Macro.escape(type)), term)
      end
    end
  end

  defp encoder_expr({:array, :u8}, value) do
    quote do: Ch.RowBinary.encode_u8_array(unquote(value))
  end

  defp encoder_expr({:nullable, type}, value) do
    present = Macro.unique_var(:present, __MODULE__)
    present_for_encoding = Macro.unique_var(:present_for_encoding, __MODULE__)
    encoded = Macro.unique_var(:encoded, __MODULE__)
    expression = encoder_expr(type, present_for_encoding)

    quote do
      case unquote(value) do
        nil ->
          1

        unquote(present) ->
          unquote(present_for_encoding) = Ch.RowBinary.Encoder.non_nil(unquote(present))

          case unquote(expression) do
            unquote(encoded) when is_list(unquote(encoded)) or is_binary(unquote(encoded)) ->
              [0 | unquote(encoded)]

            unquote(encoded) ->
              [0, unquote(encoded)]
          end
      end
    end
  end

  defp encoder_expr(:datetime, value) do
    quote do
      case unquote(value) do
        %NaiveDateTime{} = datetime ->
          {seconds, _micros} = NaiveDateTime.to_gregorian_seconds(datetime)
          <<seconds - unquote(@epoch_gregorian_seconds)::32-little>>

        %DateTime{} = datetime ->
          <<DateTime.to_unix(datetime, :second)::32-little>>

        nil ->
          <<0::32>>
      end
    end
  end

  defp encoder_expr({:datetime64, time_unit}, value) do
    quote do
      case unquote(value) do
        %NaiveDateTime{} = datetime ->
          {seconds, micros} = NaiveDateTime.to_gregorian_seconds(datetime)

          <<(seconds - unquote(@epoch_gregorian_seconds)) * unquote(time_unit) +
              div(micros * unquote(time_unit), 1_000_000)::64-little-signed>>

        %DateTime{} = datetime ->
          <<DateTime.to_unix(datetime, unquote(time_unit))::64-little-signed>>

        nil ->
          <<0::64>>
      end
    end
  end

  defp encoder_expr(:date, value) do
    quote do
      case unquote(value) do
        %Date{} = date ->
          <<Date.to_gregorian_days(date) - unquote(@epoch_gregorian_days)::16-little>>

        nil ->
          <<0::16>>
      end
    end
  end

  defp encoder_expr(:date32, value) do
    quote do
      case unquote(value) do
        %Date{} = date ->
          <<Date.to_gregorian_days(date) - unquote(@epoch_gregorian_days)::32-little-signed>>

        nil ->
          <<0::32>>
      end
    end
  end

  defp encoder_expr(:time, value) do
    quote do
      case unquote(value) do
        %Time{} = time ->
          {seconds, _micros} = Time.to_seconds_after_midnight(time)
          <<seconds::32-little-signed>>

        nil ->
          <<0::32>>
      end
    end
  end

  defp encoder_expr(type, value) do
    quote do: Ch.RowBinary.encode(unquote(Macro.escape(type)), unquote(value))
  end
end
