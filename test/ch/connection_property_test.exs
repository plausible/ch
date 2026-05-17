defmodule Ch.ConnectionPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  setup do
    {:ok, pool: start_supervised!(Ch)}
  end

  describe "query/4" do
    test "selects rows and column names", %{pool: pool} do
      assert %{names: ["one", "two"], rows: [[1, 2]]} =
               Ch.query!(pool, "SELECT 1 AS one, 2 AS two")
    end

    test "accepts iodata statements", %{pool: pool} do
      assert Ch.query!(pool, ["S", ?E, ["LEC" | "T"], " ", ~c"123"]).rows == [[123]]
    end

    test "returns ClickHouse errors", %{pool: pool} do
      assert {:error, %Ch.Error{message: message}} = Ch.query(pool, "wat")
      assert message =~ "Code: 62"
      assert message =~ "SYNTAX_ERROR"
    end

    test "reuses the pool after a query error", %{pool: pool} do
      assert {:error, %Ch.Error{}} = Ch.query(pool, "SELECT 123 + 'a'")
      assert Ch.query!(pool, "SELECT 42").rows == [[42]]
    end

    test "runs concurrent queries", %{pool: pool} do
      parent = self()

      for _ <- 1..10 do
        spawn_link(fn -> send(parent, Ch.query!(pool, "SELECT sleep(0.05)").rows) end)
      end

      assert Ch.query!(pool, "SELECT 42").rows == [[42]]

      for _ <- 1..10 do
        assert_receive [[0]]
      end
    end
  end

  describe "query params" do
    property "scalar params round-trip through ClickHouse", %{pool: pool} do
      check all {type, value, expected} <- scalar_param(),
                max_runs: 75 do
        assert Ch.query!(pool, "SELECT {value:#{type}}", %{"value" => value}).rows == [[expected]]
      end
    end

    property "array params round-trip through ClickHouse", %{pool: pool} do
      check all {type, values, expected} <- array_param(),
                max_runs: 50 do
        assert Ch.query!(pool, "SELECT {value:Array(#{type})}", %{"value" => values}).rows == [
                 [expected]
               ]
      end
    end

    test "identifier params can address tables", %{pool: pool} do
      Help.query!("DROP TABLE IF EXISTS connection_property_identifier_params")
      Help.query!("CREATE TABLE connection_property_identifier_params (a UInt8) ENGINE Memory")
      on_exit(fn -> Help.query!("DROP TABLE IF EXISTS connection_property_identifier_params") end)

      Ch.query!(pool, "INSERT INTO {table:Identifier} VALUES (1), (2)", %{
        "table" => "connection_property_identifier_params"
      })

      assert Ch.query!(pool, "SELECT sum(a) FROM {table:Identifier}", %{
               "table" => "connection_property_identifier_params"
             }).rows == [[3]]
    end
  end

  describe "RowBinary inserts" do
    property "rows encoded as RowBinary can be inserted and selected", %{pool: pool} do
      Help.query!("DROP TABLE IF EXISTS connection_property_rowbinary")

      Help.query!("""
      CREATE TABLE connection_property_rowbinary (
        id UInt8,
        name String,
        active Bool
      ) ENGINE Memory
      """)

      on_exit(fn -> Help.query!("DROP TABLE IF EXISTS connection_property_rowbinary") end)

      check all rows <- rowbinary_rows(),
                max_runs: 25 do
        rowbinary = Ch.RowBinary.encode_rows(rows, ["UInt8", "String", "Bool"])

        Ch.query!(pool, "TRUNCATE TABLE connection_property_rowbinary")

        Ch.query!(pool, [
          "INSERT INTO connection_property_rowbinary FORMAT RowBinary\n" | rowbinary
        ])

        assert Ch.query!(pool, "SELECT * FROM connection_property_rowbinary ORDER BY id").rows ==
                 Enum.sort_by(rows, &List.first/1)
      end
    end

    test "supports RowBinaryWithNamesAndTypes payloads", %{pool: pool} do
      Help.query!("DROP TABLE IF EXISTS connection_property_rowbinary_names_types")

      Help.query!("""
      CREATE TABLE connection_property_rowbinary_names_types (
        country_code FixedString(2),
        rare_string LowCardinality(String),
        maybe_int32 Nullable(Int32)
      ) ENGINE Memory
      """)

      on_exit(fn ->
        Help.query!("DROP TABLE IF EXISTS connection_property_rowbinary_names_types")
      end)

      names = ["country_code", "rare_string", "maybe_int32"]
      types = ["FixedString(2)", "LowCardinality(String)", "Nullable(Int32)"]
      rows = [["AB", "rare", -42], ["CD", "another", nil]]

      rowbinary = [
        Ch.RowBinary.encode_names_and_types(names, types)
        | Ch.RowBinary.encode_rows(rows, types)
      ]

      Ch.query!(pool, [
        "INSERT INTO connection_property_rowbinary_names_types FORMAT RowBinaryWithNamesAndTypes\n"
        | rowbinary
      ])

      assert Ch.query!(
               pool,
               "SELECT * FROM connection_property_rowbinary_names_types ORDER BY country_code"
             ).rows ==
               rows
    end
  end

  defp scalar_param do
    one_of([
      gen_constant("UInt8", integer(0..255)),
      gen_constant("Int16", integer(-32_768..32_767)),
      gen_constant("Bool", boolean()),
      gen_constant("String", safe_string()),
      gen_constant("Date", date_gen()),
      gen_constant("Date32", date32_gen()),
      gen_constant("Decimal(18, 4)", decimal_gen())
    ])
  end

  defp gen_constant(type, generator) do
    gen all value <- generator do
      expected =
        case type do
          "Decimal(18, 4)" -> Decimal.round(value, 4)
          _ -> value
        end

      {type, value, expected}
    end
  end

  defp array_param do
    one_of([
      gen_array("UInt8", integer(0..255)),
      gen_array("Int16", integer(-32_768..32_767)),
      gen_array("Bool", boolean()),
      gen_array("String", safe_string()),
      gen_array("Date", date_gen())
    ])
  end

  defp gen_array(type, generator) do
    gen all values <- list_of(generator, max_length: 8) do
      {type, values, values}
    end
  end

  defp rowbinary_rows do
    uniq_list_of(
      fixed_list([
        integer(0..255),
        safe_string(),
        boolean()
      ]),
      max_length: 12
    )
  end

  defp safe_string do
    string(:printable, max_length: 32)
  end

  defp date_gen do
    gen all days <- integer(0..20_000) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp date32_gen do
    gen all days <- integer(-25_567..120_529) do
      Date.add(~D[1970-01-01], days)
    end
  end

  defp decimal_gen do
    gen all sign <- member_of([1, -1]),
            coef <- integer(0..999_999_999),
            exp <- integer(-4..4) do
      Decimal.new(sign, coef, exp)
    end
  end
end
