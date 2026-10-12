defmodule Ch.QueryRetryTest do
  use ExUnit.Case, async: true

  alias Ch.Query

  test "recognizes ordinary read statements independently of the command option" do
    for statement <- [
          "SELECT 1",
          "show tables",
          "DESCRIBE TABLE events",
          "EXISTS TABLE events",
          "\t\r\nSeLeCt 1",
          "SELECT 1;\n",
          "WITH 1 AS n SELECT n",
          "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ] do
      assert Query.read_only?(Query.build(statement)), statement
      assert Query.read_only?(Query.build(statement, command: :custom)), statement
    end
  end

  test "inspects the entire SQL iodata" do
    assert Query.read_only?(Query.build([?S, "eL", ["e" | "cT"], " 1"]))

    refute Query.read_only?(
             Query.build(["SELECT 1 PAR", ["ALL" | "EL"], " WITH INSERT INTO t VALUES (1)"])
           )
  end

  test "does not treat command overrides as an assertion of retry safety" do
    for statement <- [
          "INSERT INTO events VALUES (1)",
          "ALTER TABLE events DELETE WHERE id = 1",
          "CREATE TABLE events (id UInt64) ENGINE Memory"
        ] do
      refute Query.read_only?(Query.build(statement, command: :select)), statement
    end
  end

  test "rejects possible writes after a read prefix" do
    for statement <- [
          "SELECT 1 PARALLEL WITH INSERT INTO events VALUES (1)",
          "SELECT 1 pArAlLeL /* comment */ WiTh INSERT INTO events VALUES (1)",
          "SHOW TABLES PARALLEL WITH DROP TABLE events",
          "WITH cte AS (SELECT 1) INSERT INTO events SELECT * FROM cte",
          "SELECT 1; INSERT INTO events VALUES (1)",
          "SELECT 1; SELECT 2",
          "SELECT 1;;",
          "SELECT 1; -- another statement could follow"
        ] do
      refute Query.read_only?(Query.build(statement)), statement
    end
  end

  test "rejects unknown prefixes and administrative statements" do
    for statement <- [
          "",
          "SELECTED 1",
          "SHOWING TABLES",
          "EXISTS_TABLE events",
          "WITHIN events",
          "WITH 1 AS n",
          "/* read */ SELECT 1",
          "-- read\nSELECT 1",
          "EXPLAIN SELECT 1",
          "CHECK TABLE events",
          "WATCH events",
          "BACKUP TABLE events TO Disk('backup', 'events')",
          "RESTORE TABLE events FROM Disk('backup', 'events')"
        ] do
      refute Query.read_only?(Query.build(statement, command: :select)), statement
    end
  end

  test "conservatively rejects keywords inside literals, identifiers, and comments" do
    for statement <- [
          "SELECT 'INSERT'",
          "SELECT `update` FROM events",
          "SELECT 1 /* DROP TABLE events */",
          "SELECT 'parallel with'",
          "WITH insert AS (SELECT 1) SELECT * FROM insert"
        ] do
      refute Query.read_only?(Query.build(statement)), statement
    end

    assert Query.read_only?(Query.build("SELECT 'updated', insertion_count FROM events"))
  end
end
