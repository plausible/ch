defmodule Ch.ErrorTest do
  use ExUnit.Case, async: true

  describe "ensure_printable/2" do
    test "leaves printable message as is" do
      printable_message =
        "Code: 57. DB::Exception: Table helloworld.my_first_table already exists. (TABLE_ALREADY_EXISTS) (version 22.10.1.1175 (official build))\n"

      %Ch.Error{message: message} = Ch.Error.exception(57, printable_message)
      assert message == printable_message
    end

    test "inspects unprintable parts" do
      unprintable_message =
        <<67, 111, 100, 101, 58, 32, 54, 50, 46, 32, 68, 66, 58, 58, 69, 120, 99, 101, 112, 116,
          105, 111, 110, 58, 32, 83, 121, 110, 116, 97, 120, 32, 101, 114, 114, 111, 114, 58, 32,
          102, 97, 105, 108, 101, 100, 32, 97, 116, 32, 112, 111, 115, 105, 116, 105, 111, 110,
          32, 55, 54, 32, 40, 39, 101, 39, 41, 32, 40, 108, 105, 110, 101, 32, 50, 44, 32, 99,
          111, 108, 32, 49, 41, 58, 32, 101, 0, 0, 0, 18, 72, 101, 108, 108, 111, 44, 32, 67, 108,
          105, 99, 107, 72, 111, 117, 115, 101, 33, 142, 154, 183, 99, 0, 0, 128, 191, 102, 0, 0,
          0, 30, 73, 110, 115, 101, 114, 116, 32, 97, 32, 108, 111, 116, 32, 111, 102, 32, 114,
          111, 119, 115, 32, 112, 101, 114, 32, 98, 97, 116, 99, 104, 0, 19, 182, 99, 213, 4, 181,
          63, 102, 0, 0, 0, 50, 83, 111, 114, 116, 32, 121, 111, 117, 114, 32, 100, 97, 116, 97,
          32, 98, 97, 115, 101, 100, 32, 111, 110, 32, 121, 111, 117, 114, 32, 99, 111, 109, 109,
          111, 110, 108, 121, 45, 117, 115, 101, 100, 32, 113, 117, 101, 114, 105, 101, 115, 128,
          100, 183, 99, 182, 243, 45, 64, 101, 0, 0, 0, 45, 71, 114, 97, 110, 117, 108, 101, 115,
          32, 97, 114, 101, 32, 116, 104, 101, 32, 115, 46, 32, 69, 120, 112, 101, 99, 116, 101,
          100, 32, 111, 110, 101, 32, 111, 102, 58, 32, 70, 82, 79, 77, 32, 73, 78, 70, 73, 76,
          69, 44, 32, 83, 69, 84, 84, 73, 78, 71, 83, 44, 32, 86, 65, 76, 85, 69, 83, 44, 32, 70,
          79, 82, 77, 65, 84, 44, 32, 83, 69, 76, 69, 67, 84, 44, 32, 87, 73, 84, 72, 44, 32, 87,
          65, 84, 67, 72, 46, 32, 40, 83, 89, 78, 84, 65, 88, 95, 69, 82, 82, 79, 82, 41, 32, 40,
          118, 101, 114, 115, 105, 111, 110, 32, 50, 50, 46, 49, 48, 46, 49, 46, 49, 49, 55, 53,
          32, 40, 111, 102, 102, 105, 99, 105, 97, 108, 32, 98, 117, 105, 108, 100, 41, 41, 10>>

      %Ch.Error{message: message} = Ch.Error.exception(164, unprintable_message)

      assert message ==
               "Code: 62. DB::Exception: Syntax error: failed at position 76 ('e') (line 2, col 1): e<<0, 0, 0, 18>>Hello, ClickHouse!<<142, 154, 183>>c<<0, 0, 128, 191>>f<<0, 0, 0, 30>>Insert a lot of rows per batch<<0, 19, 182>>c<<213, 4, 181>>?f<<0, 0, 0>>2Sort your data based on your commonly-used queries<<128>>d<<183>>c<<182, 243>>-@e<<0, 0, 0>>-Granules are the s. Expected one of: FROM INFILE, SETTINGS, VALUES, FORMAT, SELECT, WITH, WATCH. (SYNTAX_ERROR) (version 22.10.1.1175 (official build))\n"
    end
  end
end
