defmodule Ch.QueryTest do
  use ExUnit.Case, async: true
  alias Ch.Query

  test "to_string" do
    query = Query.build(["select ", 1 + ?0, ?+, 2 + ?0])
    assert to_string(query) == "select 1+2"
  end

  describe "command" do
    test "without command provided" do
      assert Query.build("select 1+2").command == :select
      assert Query.build("select 1+2").command == :select
      assert Query.build("   select 1+2").command == :select
      assert Query.build("\t\n\t\nselect 1+2").command == :select

      assert Query.build("""

             select 1+2
             """).command == :select

      assert Query.build(["select 1+2"]).command == :select

      assert Query.build("with insert as (select 1) select * from insert").command == :select
    end

    test "with nil command provided" do
      assert Query.build("select 1+2", nil).command == :select
    end

    test "with command provided" do
      assert Query.build("select 1+2", :custom).command == :custom
    end

    @tag skip: true
    test "TODO" do
      assert Query.build("Select 1+2").command == :select
    end
  end
end
