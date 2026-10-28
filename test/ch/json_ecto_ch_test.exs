defmodule Ch.JSONEctoChTest do
  use ExUnit.Case

  @moduletag :json

  defmodule Event do
    use Ecto.Schema

    @primary_key false
    schema "ch_json_events" do
      field(:id, Ch, type: "UInt32")

      field(:payload, Ch,
        type:
          "JSON(SKIP REGEXP 'tmp_.*', max_dynamic_paths = 512, SKIP secret, a.b UInt32, max_dynamic_types=16)"
      )
    end
  end

  defmodule Nested do
    use Ecto.Schema

    @primary_key false
    schema "ch_json_nested" do
      field(:id, Ch, type: "UInt32")
      field(:many, Ch, type: "Array(JSON(max_dynamic_paths=8, a UInt8))")
      field(:maybe, Ch, type: "Nullable(JSON(max_dynamic_paths=8, a UInt8))")
    end
  end

  @insert_events "INSERT INTO ch_json_events FORMAT RowBinaryWithNamesAndTypes"
  @insert_nested "INSERT INTO ch_json_nested FORMAT RowBinaryWithNamesAndTypes"

  setup do
    drop = fn ->
      Ch.Test.query("DROP TABLE IF EXISTS ch_json_events")
      Ch.Test.query("DROP TABLE IF EXISTS ch_json_nested")
    end

    drop.()
    on_exit(drop)

    Ch.Test.query("""
    CREATE TABLE ch_json_events (
      id UInt32,
      payload JSON(max_dynamic_types=16, max_dynamic_paths=512, a.b UInt32, SKIP secret, SKIP REGEXP 'tmp_.*')
    ) ENGINE MergeTree ORDER BY id
    """)

    Ch.Test.query("""
    CREATE TABLE ch_json_nested (
      id UInt32,
      many Array(JSON(max_dynamic_paths=8, a UInt8)),
      maybe Nullable(JSON(max_dynamic_paths=8, a UInt8))
    ) ENGINE MergeTree ORDER BY id
    """)

    {:ok, conn: start_supervised!({Ch, database: Ch.Test.database()})}
  end

  test "insert_all flow", %{conn: conn} do
    rows = [
      [1, %{"a" => %{"b" => 7}, "x" => "y"}],
      [2, %{"secret" => "s", "keep" => true}]
    ]

    # ecto_ch: Ecto.Adapters.ClickHouse.Schema.insert_rows/5
    Ch.query!(conn, @insert_events, rows,
      names: names(Event),
      types: ecto_ch_types(Event, [:id, :payload])
    )

    assert Ch.query!(conn, "SELECT id, payload FROM ch_json_events ORDER BY id").rows == [
             [1, %{"a" => %{"b" => 7}, "x" => "y"}],
             [2, %{"a" => %{"b" => 0}, "keep" => true}]
           ]
  end

  test "insert flow", %{conn: conn} do
    # ecto_ch: Ecto.Adapters.ClickHouse.Schema.insert/4
    Ch.query!(conn, @insert_events, [[3, %{"x" => 1}]],
      command: :insert,
      names: names(Event),
      types: ecto_ch_types(Event, [:id, :payload])
    )

    assert Ch.query!(conn, "SELECT id, payload FROM ch_json_events").rows ==
             [[3, %{"a" => %{"b" => 0}, "x" => 1}]]
  end

  test "insert_stream flow", %{conn: conn} do
    header = [:id, :payload]
    types = Ch.RowBinary.encoding_types(ecto_ch_types(Event, header))
    chunks = [[[4, %{"x" => 1}]], [[5, %{"y" => "z"}]]]

    # ecto_ch: Ecto.Adapters.ClickHouse.Schema.do_insert_stream/7
    stream =
      Stream.concat(
        [Ch.RowBinary.encode_names_and_types(names(Event), types)],
        Stream.map(chunks, &Ch.RowBinary._encode_rows(&1, types))
      )

    assert %{num_rows: 2} =
             Ch.query!(conn, @insert_events, stream, command: :insert, encode: false)

    assert Ch.query!(conn, "SELECT id, payload FROM ch_json_events ORDER BY id").rows == [
             [4, %{"a" => %{"b" => 0}, "x" => 1}],
             [5, %{"a" => %{"b" => 0}, "y" => "z"}]
           ]
  end

  test "header equals the server type", %{conn: conn} do
    for schema <- [Event, Nested], field <- schema.__schema__(:fields) do
      [type] = ecto_ch_types(schema, [field])
      table = schema.__schema__(:source)

      assert type |> Ch.Types.encode() |> IO.iodata_to_binary() ==
               server_type(conn, table, field)
    end
  end

  test "nested columns", %{conn: conn} do
    rows = [
      [1, [%{"a" => 1}, %{"a" => 2}], %{"a" => 3}],
      [2, [], nil]
    ]

    Ch.query!(conn, @insert_nested, rows,
      names: names(Nested),
      types: ecto_ch_types(Nested, [:id, :many, :maybe])
    )

    assert Ch.query!(conn, "SELECT id, many, maybe FROM ch_json_nested ORDER BY id").rows == rows
  end

  defp names(schema), do: Enum.map(schema.__schema__(:fields), &Atom.to_string/1)

  # ecto_ch unwraps {:parameterized, {Ch, type}} in remap_type/4
  defp ecto_ch_types(schema, fields) do
    for field <- fields do
      {:parameterized, {Ch, type}} = schema.__schema__(:type, field)
      type
    end
  end

  defp server_type(conn, table, column) do
    %{rows: [[type]]} =
      Ch.query!(
        conn,
        "SELECT type FROM system.columns WHERE database = currentDatabase() AND table = {t:String} AND name = {n:String}",
        %{"t" => table, "n" => Atom.to_string(column)}
      )

    type
  end
end
