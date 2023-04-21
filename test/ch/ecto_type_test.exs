defmodule Ch.EctoTypeTest do
  use ExUnit.Case, async: true

  test "String" do
    assert {:parameterized, Ch, :string} = type = Ecto.ParameterizedType.init(Ch, type: "String")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :string

    assert {:ok, "something"} = Ecto.Type.cast(type, "something")
    assert {:ok, "something"} = Ecto.Type.dump(type, "something")
    assert {:ok, "something"} = Ecto.Type.load(type, "something")
  end

  test "Nullable(String)" do
    assert {:parameterized, Ch, {:nullable, :string}} =
             type = Ecto.ParameterizedType.init(Ch, type: "Nullable(String)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :string

    assert {:ok, "something"} = Ecto.Type.cast(type, "something")
    assert {:ok, "something"} = Ecto.Type.dump(type, "something")
    assert {:ok, "something"} = Ecto.Type.load(type, "something")
  end

  test "LowCardinality(String)" do
    assert {:parameterized, Ch, {:low_cardinality, :string}} =
             type = Ecto.ParameterizedType.init(Ch, type: "LowCardinality(String)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :string

    assert {:ok, "something"} = Ecto.Type.cast(type, "something")
    assert {:ok, "something"} = Ecto.Type.dump(type, "something")
    assert {:ok, "something"} = Ecto.Type.load(type, "something")
  end

  test "Array(String)" do
    assert {:parameterized, Ch, {:array, :string}} =
             type = Ecto.ParameterizedType.init(Ch, type: "Array(String)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == {:array, :string}

    assert {:ok, ["something"]} = Ecto.Type.cast(type, ["something"])
    assert {:ok, ["something"]} = Ecto.Type.dump(type, ["something"])
    assert {:ok, ["something"]} = Ecto.Type.load(type, ["something"])
  end

  for size <- [8, 16, 32, 64, 128, 256] do
    for {encoded, decoded} <- [{"Int#{size}", :"i#{size}"}, {"UInt#{size}", :"u#{size}"}] do
      test encoded do
        assert {:parameterized, Ch, unquote(decoded)} =
                 type = Ecto.ParameterizedType.init(Ch, type: unquote(encoded))

        assert Ecto.Type.type(type) == type
        assert Ch.base_type(type) == :integer

        assert {:ok, 1} = Ecto.Type.cast(type, 1)
        assert {:ok, 1} = Ecto.Type.dump(type, 1)
        assert {:ok, 1} = Ecto.Type.load(type, 1)
      end
    end
  end

  for size <- [32, 64] do
    test "Float#{size}" do
      assert {:parameterized, Ch, unquote(:"f#{size}")} =
               type = Ecto.ParameterizedType.init(Ch, type: unquote("Float#{size}"))

      assert Ecto.Type.type(type) == type
      assert Ch.base_type(type) == :float

      assert {:ok, 1.0} = Ecto.Type.cast(type, 1.0)
      assert {:ok, 1.0} = Ecto.Type.dump(type, 1.0)
      assert {:ok, 1.0} = Ecto.Type.load(type, 1.0)
    end
  end

  test "Date" do
    assert {:parameterized, Ch, :date} = type = Ecto.ParameterizedType.init(Ch, type: "Date")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :date

    assert {:ok, ~D[2001-01-01]} = Ecto.Type.cast(type, ~D[2001-01-01])
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.cast(type, "2001-01-01")
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.dump(type, ~D[2001-01-01])
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.load(type, ~D[2001-01-01])
  end

  test "Date32" do
    assert {:parameterized, Ch, :date32} = type = Ecto.ParameterizedType.init(Ch, type: "Date32")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :date

    assert {:ok, ~D[2001-01-01]} = Ecto.Type.cast(type, ~D[2001-01-01])
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.cast(type, "2001-01-01")
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.dump(type, ~D[2001-01-01])
    assert {:ok, ~D[2001-01-01]} = Ecto.Type.load(type, ~D[2001-01-01])
  end

  test "Bool" do
    assert {:parameterized, Ch, :boolean} = type = Ecto.ParameterizedType.init(Ch, type: "Bool")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :boolean

    assert {:ok, true} = Ecto.Type.cast(type, true)
    assert {:ok, false} = Ecto.Type.cast(type, false)
    assert {:ok, true} = Ecto.Type.dump(type, true)
    assert {:ok, true} = Ecto.Type.load(type, true)
  end

  test "DateTime" do
    assert {:parameterized, Ch, :datetime} =
             type = Ecto.ParameterizedType.init(Ch, type: "DateTime")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :naive_datetime

    assert {:ok, ~N[2001-01-01 12:00:00]} = Ecto.Type.cast(type, ~N[2001-01-01 12:00:00])
    assert {:ok, ~N[2001-01-01 12:00:00]} = Ecto.Type.cast(type, "2001-01-01 12:00:00")
    assert {:ok, ~N[2001-01-01 12:00:00]} = Ecto.Type.dump(type, ~N[2001-01-01 12:00:00])
    assert {:ok, ~N[2001-01-01 12:00:00]} = Ecto.Type.load(type, ~N[2001-01-01 12:00:00])
  end

  test "DateTime('UTC')" do
    assert {:parameterized, Ch, {:datetime, "UTC"}} =
             type = Ecto.ParameterizedType.init(Ch, type: "DateTime('UTC')")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :utc_datetime

    assert {:ok, ~U[2001-01-01 12:00:00Z]} = Ecto.Type.cast(type, ~U[2001-01-01 12:00:00Z])
    assert {:ok, ~U[2001-01-01 12:00:00Z]} = Ecto.Type.cast(type, "2001-01-01 12:00:00Z")
    assert {:ok, ~U[2001-01-01 12:00:00Z]} = Ecto.Type.dump(type, ~U[2001-01-01 12:00:00Z])
    assert {:ok, ~U[2001-01-01 12:00:00Z]} = Ecto.Type.load(type, ~U[2001-01-01 12:00:00Z])
  end

  test "DateTime64(3)" do
    assert {:parameterized, Ch, {:datetime64, 3}} =
             type = Ecto.ParameterizedType.init(Ch, type: "DateTime64(3)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :naive_datetime_usec

    assert {:ok, ~N[2001-01-01 12:00:00.123456]} =
             Ecto.Type.cast(type, ~N[2001-01-01 12:00:00.123456])

    assert {:ok, ~N[2001-01-01 12:00:00.123456]} =
             Ecto.Type.cast(type, "2001-01-01 12:00:00.123456")

    assert {:ok, ~N[2001-01-01 12:00:00.123456]} =
             Ecto.Type.dump(type, ~N[2001-01-01 12:00:00.123456])

    assert {:ok, ~N[2001-01-01 12:00:00.123456]} =
             Ecto.Type.load(type, ~N[2001-01-01 12:00:00.123456])
  end

  test "DateTime64(3, 'UTC')" do
    assert {:parameterized, Ch, {:datetime64, 3, "UTC"}} =
             type = Ecto.ParameterizedType.init(Ch, type: "DateTime64(3, 'UTC')")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :utc_datetime_usec

    assert {:ok, ~U[2001-01-01 12:00:00.123456Z]} =
             Ecto.Type.cast(type, ~U[2001-01-01 12:00:00.123456Z])

    assert {:ok, ~U[2001-01-01 12:00:00.123456Z]} =
             Ecto.Type.cast(type, "2001-01-01 12:00:00.123456Z")

    assert {:ok, ~U[2001-01-01 12:00:00.123456Z]} =
             Ecto.Type.dump(type, ~U[2001-01-01 12:00:00.123456Z])

    assert {:ok, ~U[2001-01-01 12:00:00.123456Z]} =
             Ecto.Type.load(type, ~U[2001-01-01 12:00:00.123456Z])
  end

  test "SimpleAggregateFunction(any, String)" do
    assert {:parameterized, Ch, {:simple_aggregate_function, "any", :string}} =
             type = Ecto.ParameterizedType.init(Ch, type: "SimpleAggregateFunction(any, String)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :string

    assert {:ok, "something"} = Ecto.Type.cast(type, "something")
    assert {:ok, "something"} = Ecto.Type.dump(type, "something")
    assert {:ok, "something"} = Ecto.Type.load(type, "something")
  end

  test "FixedString(3)" do
    assert {:parameterized, Ch, {:fixed_string, 3}} =
             type = Ecto.ParameterizedType.init(Ch, type: "FixedString(3)")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :string

    assert {:ok, "som"} = Ecto.Type.cast(type, "som")
    assert {:ok, "som"} = Ecto.Type.dump(type, "som")
    assert {:ok, "som"} = Ecto.Type.load(type, "som")
  end

  for size <- [8, 16] do
    decoded = :"enum#{size}"
    encoded = "Enum#{size}"
    full_encoded = encoded <> "('hello' = 1, 'world' = 2)"

    test full_encoded do
      assert {:parameterized, Ch, {unquote(decoded), [{"world", 2}, {"hello", 1}]}} =
               type = Ecto.ParameterizedType.init(Ch, type: unquote(full_encoded))

      assert Ecto.Type.type(type) == type

      # TODO Ecto.Enum?
      assert Ch.base_type(type) == :string

      assert {:ok, "hello"} = Ecto.Type.cast(type, "hello")
      assert {:ok, "world"} = Ecto.Type.cast(type, "world")
      # assert {:ok, "hello"} = Ecto.Type.cast(type, 1)
      # assert {:ok, "world"} = Ecto.Type.cast(type, 2)

      assert {:ok, "hello"} = Ecto.Type.dump(type, "hello")
      assert {:ok, "world"} = Ecto.Type.dump(type, "world")
      # assert {:ok, 1} = Ecto.Type.dump(type, 1)
      # assert {:ok, 2} = Ecto.Type.dump(type, 2)

      assert {:ok, "hello"} = Ecto.Type.load(type, "hello")
      assert {:ok, "world"} = Ecto.Type.load(type, "world")
      # assert {:ok, "hello"} = Ecto.Type.load(type, 1)
      # assert {:ok, "world"} = Ecto.Type.load(type, 2)
    end
  end

  test "UUID" do
    assert {:parameterized, Ch, :uuid} = type = Ecto.ParameterizedType.init(Ch, type: "UUID")

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == Ecto.UUID

    uuid = Ecto.UUID.generate()
    bin_uuid = Ecto.UUID.dump!(uuid)

    assert {:ok, ^uuid} = Ecto.Type.cast(type, uuid)
    assert {:ok, ^uuid} = Ecto.Type.cast(type, bin_uuid)
    assert {:ok, ^bin_uuid} = Ecto.Type.dump(type, uuid)
    # assert {:ok, ^uuid} = Ecto.Type.load(type, uuid)
    assert {:ok, ^uuid} = Ecto.Type.load(type, bin_uuid)
  end

  @tag skip: true
  test "IPv4"

  @tag skip: true
  test "IPv6"

  test "Decimal(18, 4)" do
    assert {:parameterized, Ch, {:decimal, 18, 4}} =
             type = Ecto.ParameterizedType.init(Ch, type: unquote("Decimal(18, 4)"))

    assert Ecto.Type.type(type) == type
    assert Ch.base_type(type) == :decimal

    assert {:ok, %Decimal{}} = Ecto.Type.cast(type, 1.0)
    assert {:ok, %Decimal{}} = Ecto.Type.dump(type, 1.0)
    assert {:ok, %Decimal{}} = Ecto.Type.load(type, 1.0)
  end

  for size <- [32, 64, 128, 256] do
    test "Decimal#{size}(4)" do
      assert {:parameterized, Ch, {unquote(:"decimal#{size}"), 4}} =
               type = Ecto.ParameterizedType.init(Ch, type: unquote("Decimal#{size}(4)"))

      assert Ecto.Type.type(type) == type
      assert Ch.base_type(type) == :decimal

      assert {:ok, %Decimal{}} = Ecto.Type.cast(type, 1.0)
      assert {:ok, %Decimal{}} = Ecto.Type.dump(type, 1.0)
      assert {:ok, %Decimal{}} = Ecto.Type.load(type, 1.0)
    end
  end
end
