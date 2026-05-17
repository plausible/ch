defmodule Ch.QueryTest do
  use ExUnit.Case, async: true

  # adapted from https://github.com/elixir-ecto/postgrex/blob/master/test/query_test.exs
  describe "query" do
    setup do
      pool = start_supervised!(Ch)
      {:ok, pool: pool, conn: pool, query_options: []}
    end

    test "iodata", %{conn: conn, query_options: query_options} do
      assert [[123]] =
               Ch.query!(conn, ["S", ?E, ["LEC" | "T"], " ", ~c"123"], [], query_options).rows
    end

    test "decode basic types", %{conn: conn, query_options: query_options} do
      assert [[nil]] = Ch.query!(conn, "SELECT NULL", [], query_options).rows
      assert [[true, false]] = Ch.query!(conn, "SELECT true, false", [], query_options).rows
      assert [["e"]] = Ch.query!(conn, "SELECT 'e'::char", [], query_options).rows
      assert [["ẽ"]] = Ch.query!(conn, "SELECT 'ẽ'::char", [], query_options).rows
      assert [[42]] = Ch.query!(conn, "SELECT 42", [], query_options).rows
      assert [[42.0]] = Ch.query!(conn, "SELECT 42::float", [], query_options).rows
      assert [[42.0]] = Ch.query!(conn, "SELECT 42.0", [], query_options).rows
      # TODO [[:NaN]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT 'NaN'::float", [], query_options).rows
      # TODO [[:int]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT 'inf'::float", [], query_options).rows
      # TODO [[:"-inf"]] ?
      assert [[nil]] = Ch.query!(conn, "SELECT '-inf'::float", [], query_options).rows
      assert [["ẽric"]] = Ch.query!(conn, "SELECT 'ẽric'", [], query_options).rows
      assert [["ẽric"]] = Ch.query!(conn, "SELECT 'ẽric'::varchar", [], query_options).rows
      # TODO
      # assert [[<<1, 2, 3>>]] = Ch.query!(conn, "SELECT '\\001\\002\\003'::bytea").rows
    end

    test "decode numeric", %{conn: conn, query_options: query_options} do
      assert [[Decimal.new("42.0000000000")]] ==
               Ch.query!(conn, "SELECT 42::numeric(10,10)", [], query_options).rows
    end

    @tag skip: true
    test "decode json/jsonb", %{conn: conn, query_options: query_options} do
      assert_raise ArgumentError, "Object('json') type is not supported", fn ->
        assert [[%{"foo" => 42}]] ==
                 Ch.query!(conn, "SELECT '{\"foo\": 42}'::json", [], query_options).rows
      end
    end

    test "decode uuid", %{conn: conn, query_options: query_options} do
      uuid = <<160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17>>

      assert [[^uuid]] =
               Ch.query!(
                 conn,
                 "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID",
                 [],
                 query_options
               ).rows
    end

    # https://clickhouse.com/docs/sql-reference/data-types/time
    @tag :time
    test "decode time", %{conn: conn, query_options: query_options} do
      settings = [enable_time_time64_type: 1]

      times = [
        %{value: "00:00:00", expected: ~T[00:00:00]},
        %{value: "12:34:56", expected: ~T[12:34:56]},
        %{value: "23:59:59", expected: ~T[23:59:59]}
      ]

      for time <- times do
        %{value: value, expected: expected} = time

        assert Ch.query!(
                 conn,
                 "SELECT '#{value}'::time",
                 [],
                 Keyword.merge(query_options, settings: settings)
               ).rows ==
                 [[expected]]

        assert Ch.query!(
                 conn,
                 "SELECT {time:Time}",
                 %{"time" => expected},
                 Keyword.merge(query_options, settings: settings)
               ).rows ==
                 [[expected]]
      end

      # ClickHouse supports Time values of [-999:59:59, 999:59:59]
      # and Elixir's Time supports values of [00:00:00, 23:59:59]
      # so we raise an error when ClickHouse's Time value is out of Elixir's Time range

      assert_raise ArgumentError,
                   "ClickHouse Time value -1.0 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '-00:00:01'::time",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end

      assert_raise ArgumentError,
                   "ClickHouse Time value 3599999.0 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '999:59:59'::time",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end

      assert_raise ArgumentError,
                   "ClickHouse Time value -3599999.0 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '-999:59:59'::time",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end

      # ** (Ch.Error) Code: 457. DB::Exception: Value 12:34:56.123456 cannot be parsed as Time for query parameter 'time'
      #               because it isn't parsed completely: only 8 of 15 bytes was parsed: 12:34:56. (BAD_QUERY_PARAMETER)
      #               (version 25.6.3.116 (official build))
      assert_raise Ch.Error, ~r/only 8 of 15 bytes was parsed/, fn ->
        Ch.query!(
          conn,
          "SELECT {time:Time}",
          %{"time" => ~T[12:34:56.123456]},
          Keyword.merge(query_options, settings: settings)
        )
      end
    end

    # https://clickhouse.com/docs/sql-reference/data-types/time64
    @tag :time
    test "decode time64", %{conn: conn, query_options: query_options} do
      settings = [enable_time_time64_type: 1]

      times = [
        %{value: "00:00:00.000000000", precision: 0, expected: ~T[00:00:00]},
        %{value: "12:34:56.123456789", precision: 0, expected: ~T[12:34:56]},
        %{value: "23:59:59.999999999", precision: 0, expected: ~T[23:59:59]},
        %{value: "12:34:56.123456789", precision: 1, expected: ~T[12:34:56.1]},
        %{value: "23:59:59.999999999", precision: 1, expected: ~T[23:59:59.9]},
        %{value: "12:34:56.123456789", precision: 2, expected: ~T[12:34:56.12]},
        %{value: "23:59:59.999999999", precision: 2, expected: ~T[23:59:59.99]},
        %{value: "12:34:56.123456789", precision: 3, expected: ~T[12:34:56.123]},
        %{value: "23:59:59.999999999", precision: 3, expected: ~T[23:59:59.999]},
        %{value: "12:34:56.123456789", precision: 4, expected: ~T[12:34:56.1234]},
        %{value: "23:59:59.999999999", precision: 4, expected: ~T[23:59:59.9999]},
        %{value: "12:34:56.001200000", precision: 4, expected: ~T[12:34:56.0012]},
        %{value: "12:34:56.123456789", precision: 5, expected: ~T[12:34:56.12345]},
        %{value: "23:59:59.999999999", precision: 5, expected: ~T[23:59:59.99999]},
        %{value: "12:34:56.123456789", precision: 6, expected: ~T[12:34:56.123456]},
        %{value: "12:34:56.123000", precision: 6, expected: ~T[12:34:56.123000]},
        %{value: "12:34:56.000123000", precision: 6, expected: ~T[12:34:56.000123]},
        %{value: "00:00:00.000000000", precision: 6, expected: ~T[00:00:00.000000]},
        %{value: "12:34:56.123456789", precision: 6, expected: ~T[12:34:56.123456]},
        %{value: "00:00:00.123000", precision: 6, expected: ~T[00:00:00.123000]},
        %{value: "00:00:00.000123000", precision: 6, expected: ~T[00:00:00.000123]},
        %{value: "23:59:59.999999999", precision: 6, expected: ~T[23:59:59.999999]},
        %{value: "12:34:56.123456789", precision: 7, expected: ~T[12:34:56.123456]},
        %{value: "12:34:56.123456789", precision: 8, expected: ~T[12:34:56.123456]},
        %{value: "12:34:56.123456789", precision: 9, expected: ~T[12:34:56.123456]},
        %{value: "23:59:59.999999999", precision: 9, expected: ~T[23:59:59.999999]}
      ]

      for time <- times do
        %{value: value, precision: precision, expected: expected} = time

        assert Ch.query!(
                 conn,
                 "SELECT '#{value}'::time64(#{precision})",
                 [],
                 Keyword.merge(query_options, settings: settings)
               ).rows ==
                 [[expected]]

        assert Ch.query!(
                 conn,
                 "SELECT {time:time64(#{precision})}",
                 %{"time" => expected},
                 Keyword.merge(query_options, settings: settings)
               ).rows ==
                 [[expected]]
      end

      # ClickHouse supports Time64 values of [-999:59:59.999999999, 999:59:59.999999999]
      # and Elixir's Time supports values of [00:00:00.000000, 23:59:59.999999]
      # so we raise an error when ClickHouse's Time64 value is out of Elixir's Time range

      assert_raise ArgumentError,
                   "ClickHouse Time value -1.0 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '-00:00:01.000'::time64(6)",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end

      assert_raise ArgumentError,
                   "ClickHouse Time value 3599999.999999 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '999:59:59.999999999'::time64(6)",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end

      assert_raise ArgumentError,
                   "ClickHouse Time value -3599999.999999 (seconds) is out of Elixir's Time range (00:00:00.000000 - 23:59:59.999999)",
                   fn ->
                     Ch.query!(
                       conn,
                       "SELECT '-999:59:59.999999999'::time64(6)",
                       [],
                       Keyword.merge(query_options, settings: settings)
                     )
                   end
    end

    test "decode arrays", %{conn: conn, query_options: query_options} do
      assert [[[]]] = Ch.query!(conn, "SELECT []", [], query_options).rows
      assert [[[1]]] = Ch.query!(conn, "SELECT [1]", [], query_options).rows
      assert [[[1, 2]]] = Ch.query!(conn, "SELECT [1,2]", [], query_options).rows
      assert [[[[0], [1]]]] = Ch.query!(conn, "SELECT [[0],[1]]", [], query_options).rows
      assert [[[[0]]]] = Ch.query!(conn, "SELECT [[0]]", [], query_options).rows
    end

    test "decode tuples", %{conn: conn, query_options: query_options} do
      assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello', 123)", [], query_options).rows

      assert [[{"Hello", 123}]] =
               Ch.query!(conn, "select ('Hello' as a, 123 as b)", [], query_options).rows

      assert [[{"Hello", 123}]] =
               Ch.query!(conn, "select ('Hello' as a_, 123 as b)", [], query_options).rows

      # TODO
      # assert [[{"Hello", 123}]] = Ch.query!(conn, "select ('Hello' as a$, 123 as b)", [], query_options).rows
    end

    test "decode network types", %{conn: conn, query_options: query_options} do
      assert [[{127, 0, 0, 1} = ipv4]] =
               Ch.query!(conn, "SELECT '127.0.0.1'::inet4", [], query_options).rows

      assert :inet.ntoa(ipv4) == ~c"127.0.0.1"

      assert [[{0, 0, 0, 0, 0, 0, 0, 1} = ipv6]] =
               Ch.query!(conn, "SELECT '::1'::inet6", [], query_options).rows

      assert :inet.ntoa(ipv6) == ~c"::1"

      assert [[ipv6]] =
               Ch.query!(conn, "SELECT '2001:44c8:129:2632:33:0:252:2'::inet6", [], query_options).rows

      assert :inet.ntoa(ipv6) == ~c"2001:44c8:129:2632:33:0:252:2"
    end

    test "decoded binaries copy behaviour", %{conn: conn, query_options: query_options} do
      text = "hello world"

      assert [[bin]] =
               Ch.query!(conn, "SELECT {text:String}", %{"text" => text}, query_options).rows

      assert bin == text
      assert :binary.referenced_byte_size(bin) == :binary.referenced_byte_size("hello world")

      # For OTP 20+ refc binaries up to 64 bytes might be copied during a GC
      text = String.duplicate("hello world", 6)

      assert [[bin]] =
               Ch.query!(conn, "SELECT {text:String}", %{"text" => text}, query_options).rows

      assert bin == text
    end

    test "encode basic types", %{conn: conn, query_options: query_options} do
      # TODO
      # assert [[nil, nil]] = query("SELECT $1::text, $2::int", [nil, nil])
      assert [[true, false]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Bool}, {b:Bool}",
                 %{"a" => true, "b" => false},
                 query_options
               ).rows

      assert [["ẽ"]] = Ch.query!(conn, "SELECT {s:String}", %{"s" => "ẽ"}, query_options).rows
      assert [[42]] = Ch.query!(conn, "SELECT {i:Int32}", %{"i" => 42}, query_options).rows

      assert [[42.0, 43.0]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Float64}, {b:Float64}",
                 %{"a" => 42, "b" => 43.0},
                 query_options
               ).rows

      assert [[nil, nil]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Float64}, {b:Float64}",
                 %{"a" => "NaN", "b" => "nan"},
                 query_options
               ).rows

      assert [[nil]] = Ch.query!(conn, "SELECT {f:Float64}", %{"f" => "inf"}, query_options).rows
      assert [[nil]] = Ch.query!(conn, "SELECT {f:Float64}", %{"f" => "-inf"}, query_options).rows

      assert [["ẽric"]] =
               Ch.query!(conn, "SELECT {s:String}", %{"s" => "ẽric"}, query_options).rows

      assert [[<<1, 2, 3>>]] =
               Ch.query!(conn, "SELECT {b:String}", %{"b" => <<1, 2, 3>>}, query_options).rows
    end

    test "encode numeric", %{conn: conn, query_options: query_options} do
      nums = [
        {"42", "numeric(2,0)"},
        {"0.4242", "numeric(4,4)"},
        {"42.4242", "numeric(6,4)"},
        {"1.001", "numeric(4,3)"},
        {"1.00123", "numeric(6,5)"},
        {"0.01", "numeric(3,2)"},
        {"0.00012345", "numeric(9,8)"},
        {"1000000000", "numeric(10,0)"},
        {"1000000000.0", "numeric(11,1)"},
        {"123456789123456789123456789", "numeric(27,0)"},
        {"123456789123456789123456789.123456789", "numeric(36,9)"},
        {"1.1234500000", "numeric(11,10)"},
        {"1.0000000000", "numeric(11,10)"},
        {"1.111101", "numeric(7,6)"},
        {"1.1111111101", "numeric(11,10)"},
        {"1.11110001", "numeric(9,8)"},
        # {"NaN", "numeric(1,0)"},
        {"-42", "numeric(2,0)"}
      ]

      Enum.each(nums, fn {num, type} ->
        dec = Decimal.new(num, max_digits: :infinity, max_exponent: :infinity)
        assert [[dec]] == Ch.query!(conn, "SELECT {d:#{type}}", %{"d" => dec}, query_options).rows
      end)
    end

    test "encode integers and floats as numeric", %{conn: conn, query_options: query_options} do
      dec = Decimal.new(1)

      assert [[dec]] ==
               Ch.query!(conn, "SELECT {d:numeric(1,0)}", %{"d" => 1}, query_options).rows

      dec = Decimal.from_float(1.0)

      assert [[dec]] ==
               Ch.query!(conn, "SELECT {d:numeric(2,1)}", %{"d" => 1.0}, query_options).rows
    end

    @tag skip: true
    test "encode json/jsonb", %{conn: conn, query_options: query_options} do
      json = %{"foo" => 42}
      assert [[json]] == Ch.query!(conn, "SELECT {$0::json}", [json], query_options).rows
    end

    test "encode uuid", %{conn: conn, query_options: query_options} do
      # TODO
      uuid = <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15>>
      uuid_hex = "00010203-0405-0607-0809-0a0b0c0d0e0f"

      assert [[^uuid]] =
               Ch.query!(conn, "SELECT {uuid:UUID}", %{"uuid" => uuid_hex}, query_options).rows
    end

    test "encode arrays", %{conn: conn, query_options: query_options} do
      assert [[[]]] =
               Ch.query!(conn, "SELECT {a:Array(Int32)}", %{"a" => []}, query_options).rows

      assert [[[1]]] =
               Ch.query!(conn, "SELECT {a:Array(Int32)}", %{"a" => [1]}, query_options).rows

      assert [[[1, 2]]] =
               Ch.query!(conn, "SELECT {a:Array(Int32)}", %{"a" => [1, 2]}, query_options).rows

      assert [[["1"]]] =
               Ch.query!(conn, "SELECT {a:Array(String)}", %{"a" => ["1"]}, query_options).rows

      assert [[[true]]] =
               Ch.query!(conn, "SELECT {a:Array(Bool)}", %{"a" => [true]}, query_options).rows

      assert [[[~D[2023-01-01]]]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Array(Date)}",
                 %{"a" => [~D[2023-01-01]]},
                 query_options
               ).rows

      assert [[[~U[2023-01-01 12:00:00Z]]]] ==
               Ch.query!(
                 conn,
                 "SELECT {a:Array(DateTime('UTC'))}",
                 %{"a" => [~U[2023-01-01 12:00:00Z]]},
                 query_options
               ).rows

      assert [[[[0], [1]]]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Array(Array(Int32))}",
                 %{"a" => [[0], [1]]},
                 query_options
               ).rows

      assert [[[[0]]]] =
               Ch.query!(
                 conn,
                 "SELECT {a:Array(Array(Int32))}",
                 %{"a" => [[0]]},
                 query_options
               ).rows

      # assert [[[1, nil, 3]]] = Ch.query!(conn, "SELECT {$0:Array(integer)}", [[1, nil, 3]], query_options).rows
    end

    test "encode network types", %{conn: conn, query_options: query_options} do
      # TODO, or wrap in custom struct like in postgrex
      # assert [["127.0.0.1/32"]] =
      #          Ch.query!(conn, "SELECT {$0:inet4}::text", [{127, 0, 0, 1}], query_options).rows

      assert [[{127, 0, 0, 1}]] =
               Ch.query!(conn, "SELECT {ip:String}::IPv4", %{"ip" => "127.0.0.1"}, query_options).rows

      assert [[{0, 0, 0, 0, 0, 0, 0, 1}]] =
               Ch.query!(conn, "SELECT {ip:String}::IPv6", %{"ip" => "::1"}, query_options).rows
    end

    test "result struct", %{conn: conn, query_options: query_options} do
      assert {:ok, res} = Ch.query(conn, "SELECT 123 AS a, 456 AS b", [], query_options)
      assert res.names == ["a", "b"]
      assert res.rows == [[123, 456]]
    end

    test "empty result struct", %{conn: conn, query_options: query_options} do
      assert %{names: ["number", "b"], rows: []} =
               res = Ch.query!(conn, "select number, 'a' as b from numbers(0)", [], query_options)

      assert res.rows == []
    end

    test "error struct", %{conn: conn, query_options: query_options} do
      assert {:error, %Ch.Error{}} = Ch.query(conn, "SELECT 123 + 'a'", [], query_options)
    end

    test "error code", %{conn: conn, query_options: query_options} do
      assert {:error, %Ch.Error{code: code, message: message}} =
               Ch.query(conn, "wat", [], query_options)

      assert is_nil(code) or code == 62
      assert message =~ "Code: 62"
    end

    test "connection works after failure in execute", %{conn: conn, query_options: query_options} do
      assert {:error, %Ch.Error{}} = Ch.query(conn, "wat", [], query_options)
      assert [[42]] = Ch.query!(conn, "SELECT 42", [], query_options).rows
    end

    test "async test", %{conn: conn, query_options: query_options} do
      self_pid = self()

      Enum.each(1..10, fn _ ->
        spawn_link(fn ->
          send(self_pid, Ch.query!(conn, "SELECT sleep(0.05)", [], query_options).rows)
        end)
      end)

      assert [[42]] = Ch.query!(conn, "SELECT 42", [], query_options).rows

      Enum.each(1..10, fn _ ->
        assert_receive [[0]], :timer.seconds(1)
      end)
    end
  end

  test "query before and after idle worker timeout" do
    opts = [worker_idle_timeout: 1]
    {:ok, pid} = Ch.start_link(opts)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
    :timer.sleep(20)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
    :timer.sleep(20)
    assert {:ok, _} = Ch.query(pid, "SELECT 42")
  end
end
