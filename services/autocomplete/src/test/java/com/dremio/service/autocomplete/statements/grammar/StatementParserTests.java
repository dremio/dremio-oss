/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.autocomplete.statements.grammar;

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.junit.Test;

import com.dremio.service.autocomplete.statements.visitors.StatementSerializer;
import com.dremio.test.GoldenFileTestBuilder;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.google.common.collect.ImmutableList;

public final class StatementParserTests {
  @Test
  public void statementListTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add("Empty String", MultiLineString.create("^"))
      .add("Only semicolons and cursor after one of them", MultiLineString.create(";^;;;"))
      .add("Ends with semicolon and cursor after", MultiLineString.create(";;;;^"))
      .add("Starts with semicolon and cursor before", MultiLineString.create("^;;;;"))
      .add("Statement with semicolon", MultiLineString.create("Select ^ FROM; SELECT"))
      .add("Statement with semicolon and cursor in the end", MultiLineString.create("Select FROM; SELECT FROM^"))
      .add("Statement with semicolon and cursor in the second query", MultiLineString.create("Select WHERE; SELECT ^ FROM"))
      .add("Statement with semicolon and cursor before second query", MultiLineString.create("Select FROM;^SELECT WHERE"))
      .add("Statement with semicolons and cursor in the end", MultiLineString.create("Select WHERE;SELECT FROM;^"))
      .add("Statement without semicolon", MultiLineString.create("Select * FROM^"))
      .runTests();
  }

  @Test
  public void queryStatementTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add("JUST SELECT", MultiLineString.create("SELECT ^"))
      .add("JUST SELECT *", MultiLineString.create("SELECT * ^"))
      .add("JUST SELECT * FROM ", MultiLineString.create("SELECT * FROM ^"))
      .add("BASIC QUERY", MultiLineString.create("SELECT * FROM EMP ^"))
      .add("WITH CLAUSES", MultiLineString.create("SELECT * FROM EMP WHERE EMP.age = 42 ^"))
      .add("Subquery + Cursor Inside",
        MultiLineString.create("SELECT (SELECT ^ FROM EMP) \n" +
          "FROM DEPT"))
      .add("Subquery + Cursor Outside",
        MultiLineString.create("SELECT ^, (SELECT * FROM EMP) \n" +
          "FROM DEPT"))
      .add("Subquery with function",
        MultiLineString.create("SELECT DEPTNO, (\n" +
          "SELECT MAX(EMP.SAL), ^ FROM EMP\n" +
          ") FROM DEPT"))
      .add("Middle Query", MultiLineString.create("SELECT DEPTNO, (SELECT ^ , (SELECT * FROM SALGRADE) FROM EMP) FROM DEPT"))
      .add("Subqueries projection",
        MultiLineString.create("SELECT \n" +
          "\t(SELECT * FROM EMP), \n" +
          "\t(SELECT * FROM EMP), \n" +
          "\t(SELECT * FROM EMP) \n" +
          "FROM DEPT ^"))
      .add("Subqueries from",
        MultiLineString.create(
          "SELECT * \n" +
          "FROM (SELECT * FROM EMP) as emp, (SELECT * FROM DEPT) as dept ^"))
      .add("Subqueries both", MultiLineString.create("SELECT \n" +
        "\t(SELECT * FROM emp), \n" +
        "\t(SELECT * FROM emp), \n" +
        "\t(SELECT * FROM emp) \n" +
        "FROM (SELECT * FROM EMP) as emp, (SELECT * FROM DEPT) as dept ^"))
      .add(
        "SET QUERY (UNION)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "SET QUERY (EXCEPT)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " EXCEPT\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "SET QUERY (MINUS)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " MINUS\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "SET QUERY (INTERSECT)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " INTERSECT\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "SET QUERY FIRST",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP\n" +
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "SET QUERY MIDDLE",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO, ^ FROM EMP\n" +
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "SET QUERY LAST",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP\n" +
          " UNION\n" +
          "SELECT GRADE, ^ FROM SALGRADE"))
      .add("Complex multiline query", MultiLineString.create(
        "SELECT DEPTNO FROM DEPT\n" +
          "UNION\n" +
          "SELECT DEPTNO, NAME, " +
          "  (SELECT MAX(EMP.SAL), ^\n" +
          "    FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO\n" +
          "    WHERE EMP.DEPTNO = DEPT.DEPTNO" +
          "  ) AS MAX_SAL_IN_DEPTNO\n" +
          "FROM DEPT\n" +
          "UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add("Cursor after subselect",
        MultiLineString.create("SELECT * FROM (SELECT * FROM ORGS), (SELECT * FROM SALES), ^"))
      .add("Statement with nested selects in FROM",
        MultiLineString.create("SELECT * FROM (SELECT (SELECT (SELECT 1, 2, 3)^, 4)) FROM EMP)"))
      .add("Statement with nested selects in projection",
        MultiLineString.create("SELECT (SELECT (SELECT 1, 2, 3^), 4)) FROM TEST"))
      .add("Statement with open parenthesis recognized as part of the same scope",
        MultiLineString.create("SELECT COUNT(^ FROM EMP"))
      .add("Cursor belongs to the succeeding scope if it points immediately before the scope keyword",
        MultiLineString.create("SELECT * FROM SALES ^SELECT name FROM EMP"))
      .add("SUBQUERY AT ROOT LEVEL", MultiLineString.create("(SELECT 1)^"))
      .runTests();
  }

  @Test
  public void fromClauseExtraction() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add(
        "NO FROM CLAUSE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT *^"))
      .add(
        "ONLY FROM CLAUSE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP^"))
      .add(
        "FROM + WHERE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE age < 10^"))
      .add(
        "FROM + ORDER BY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP ORDER BY age^"))
      .add(
        "FROM + LIMIT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP LIMIT 10^"))
      .add(
        "FROM + OFFSET",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP OFFSET 10^"))
      .add(
        "FROM + FETCH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP FETCH FIRST 10 ONLY^"))
      .add(
        "MULTIPLE CLAUSES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE age < 10 ORDER by age LIMIT 10 OFFSET 10 FETCH FIRST 10 ONLY^"))
      .add(
        "FROM CLAUSE WITH COMMAS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP, DEPT^"))
      .add(
        "FROM CLAUSE WITH JOINS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO^"))
      .add(
        "FROM CLAUSE WITH INCOMPLETE JOIN CONDITION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = ^"))
      .add(
        "FROM CLAUSE WITH MULTIPLE JOINS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = ^ JOIN SALGRADE ON SALGRADE.GRADE = DEPT.DEPTNO"))
      .add(
        "SUBQUERY WITH CURSOR IN OUTER QUERY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT DEPTNO, ^, NAME, (SELECT MAX(EMP.SAL)\n" +
          " FROM EMP\n" +
          " WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO\n" +
          "FROM DEPT"))
      .add(
        "Cursor after subselect",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM (SELECT * FROM ORGS), (SELECT * FROM SALES), ^"))
      .runTests();
  }

  @Test
  public void dropStatementTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add("JUST DROP", MultiLineString.create("DROP^"))
      .add("DROP INCOMPLETE / UNKNOWN TYPE", MultiLineString.create("DROP BR^"))
      .add("DROP BRANCH", MultiLineString.create("DROP BRANCH^"))
      .add("DROP ROLE", MultiLineString.create("DROP ROLE^"))
      .add("DROP TABLE", MultiLineString.create("DROP TABLE^"))
      .add("DROP TAG", MultiLineString.create("DROP TAG^"))
      .add("DROP VDS", MultiLineString.create("DROP VDS^"))
      .add("DROP VIEW", MultiLineString.create("DROP VIEW^"))
      .add("IF EXISTS", MultiLineString.create("DROP VIEW IF EXISTS^"))
      .add("WITH CATALOG PATH", MultiLineString.create("DROP VIEW myspace.myfolder.myview^"))
      .add("WITH CATALOG PATH AND IF EXISTS", MultiLineString.create("DROP VIEW IF EXISTS myspace.myfolder.myview^"))
      .runTests();
  }

  @Test
  public void deleteStatementTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add("DELETE", MultiLineString.create("DELETE^"))
      .add("DELETE + PARTIAL FROM", MultiLineString.create("DELETE FR^"))
      .add("DELETE + FROM", MultiLineString.create("DELETE FROM^"))
      .add("DELETE + FROM + TABLE", MultiLineString.create("DELETE FROM EMP^"))
      .add("DELETE + FROM + TABLE + AS", MultiLineString.create("DELETE FROM EMP AS^"))
      .add("DELETE + FROM + TABLE + AS + ALIAS", MultiLineString.create("DELETE FROM EMP AS e^"))
      .add("DELETE + FROM + TABLE + WHERE", MultiLineString.create("DELETE FROM EMP WHERE^"))
      .add("DELETE + FROM + TABLE + WHERE + CONDITION", MultiLineString.create("DELETE FROM EMP WHERE EMP.NAME = 'Brandon'^"))
      .allowExceptions()
      .runTests();
  }

  @Test
  public void updateStatementTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add(
        "UPDATE",
        MultiLineString.create("UPDATE ^"))
      .add(
        "UPDATE + TABLE",
        MultiLineString.create("UPDATE EMP ^"))
      .add(
        "UPDATE + TABLE + SET",
        MultiLineString.create("UPDATE EMP SET ^"))
      .add(
        "UPDATE + TABLE + SET + ASSIGN",
        MultiLineString.create("UPDATE EMP SET NAME = 'Brandon' ^"))
      .add(
        "UPDATE + TABLE + SET + ASSIGN LIST",
        MultiLineString.create("UPDATE EMP SET NAME = 'Brandon', AGE = 27 ^"))
      .add(
        "UPDATE + TABLE + SET + ASSIGN LIST + WHERE",
        MultiLineString.create("UPDATE EMP SET NAME = 'Brandon', AGE = 27 WHERE ^"))
      .add(
        "UPDATE + TABLE + SET + ASSIGN LIST + WHERE + BOOLEAN EXPRESSION",
        MultiLineString.create("UPDATE EMP SET NAME = 'Brandon', AGE = 27 WHERE NAME != 'Brandon' ^"))
      .runTests();
  }

  @Test
  public void rawReflectionCreateTests() {
    GoldenFileTestBuilder<MultiLineString, MultiLineString> builder = new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add(
        "USING",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING"))
      .add(
        "DISPLAY OPEN PARENS",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY("))
      .add(
        "DISPLAY FIELD",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n"+
          "DISPLAY(FIELD"))
      .add(
        "DISPLAY FIELD FINISHED",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n"+
          "DISPLAY(FIELD,"))
      .add(
        "DISPLAY FIELDS",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n"+
          "DISPLAY(FIELD, FIELD2"))
      .add(
        "DISPLAY FIELDS FINISHED",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n"+
          "DISPLAY (FIELD, FIELD2)"));
    List<ImmutableList<String>> fieldTokensList = new ImmutableList.Builder<ImmutableList<String>>()
      .add(ImmutableList.of("DISTRIBUTE", "BY"))
      .add(ImmutableList.of("STRIPED", "PARTITION", "BY"))
      .add(ImmutableList.of("CONSOLIDATED", "PARTITION", "BY"))
      .add(ImmutableList.of("LOCALSORT", "BY"))
      .build();

    for (List<String> fieldTokens : fieldTokensList) {
      for (int i = 1; i <= fieldTokens.size(); i++) {
        List<String> prefix = fieldTokens.subList(0, i);
        builder.add(
          String.join("+", prefix),
          MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
            "USING \n" +
            "DISPLAY(FIELD, FIELD2)\n"+
            String.join(" ", prefix)));
      }

      String listName =  String.join(" ", fieldTokens);
      builder
        .add(
        listName + " OPEN PARENS",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(FIELD, FIELD2)\n"+
          listName + "("))
        .add(
          listName + " FIELD",
          MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
            "USING \n" +
            "DISPLAY(FIELD, FIELD2)\n" +
            listName + "(FIELD"))
        .add(
          listName + " FIELD FINISHED",
          MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
            "USING \n" +
            "DISPLAY(FIELD, FIELD2)\n" +
            listName + "(FIELD, "))
        .add(
          listName + " FIELDS",
          MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
            "USING \n" +
            "DISPLAY(FIELD, FIELD2)\n" +
            listName + "(FIELD, FIELD2"))
        .add(
          listName + " FIELDS FINISHED",
          MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
            "USING \n" +
            "DISPLAY(FIELD, FIELD2)\n" +
            listName + "(FIELD, FIELD2)"));
    }

    builder
      .add(
        "EVERYTHING",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(FIELD, FIELD2)\n" +
          "DISTRIBUTE BY(FIELD, FIELD2)\n" +
          "PARTITION BY(FIELD, FIELD2)\n" +
          "LOCALSORT BY(FIELD, FIELD2)\n" +
          "ARROW CACHE true"))
      .add(
        "SUFFIX",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(FIELD, FIELD2)\n" +
          "PARTITION BY(FIELD, FIELD2)\n" +
          "LOCALSORT BY(FIELD, FIELD2)\n" +
          "ARROW CACHE true"))
      .add(
        "INVALID ORDER",
        MultiLineString.create("ALTER TABLE myTable CREATE RAW REFLECTION myReflection\n" +
          "USING \n" +
          "DISPLAY(FIELD, FIELD2)\n" +
          "LOCALSORT BY(FIELD, FIELD2)\n" +
          "PARTITION BY(FIELD, FIELD2)\n" +
          "ARROW CACHE true"))
      .runTests();
  }

  @Test
  public void aggregateReflectionCreateTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add(
        "USING",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING"))
      .add(
        "DIMENSIONS FIELD",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD)\n" +
          "MEASURES(FIELD)"))
      .add(
        "DIMENSIONS FIELD BY DAY",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD BY DAY)\n" +
          "MEASURES(FIELD)"))
      .add(
        "DIMENSIONS MIXED",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD BY DAY, FIELD2)\n" +
          "MEASURES(FIELD)"))
      .add(
        "MEASURES FIELD",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD)\n" +
          "MEASURES(FIELD)"))
      .add(
        "MEASURES WITH ANNOTATIONS",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD)\n" +
          "MEASURES(FIELD (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT))"))
      .add(
        "MEASURES MIXED",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD)\n" +
          "MEASURES(FIELD (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT), FIELD2)"))
      .add(

        "EVERYTHING",
        MultiLineString.create("ALTER TABLE myTable CREATE AGGREGATE REFLECTION myReflection\n" +
          "USING \n"+
          "DIMENSIONS(FIELD BY DAY, FIELD2)\n" +
          "MEASURES(FIELD (COUNT, MIN, MAX, SUM, APPROXIMATE COUNT DISTINCT), FIELD2)\n" +
          "DISTRIBUTE BY(FIELD, FIELD2)\n" +
          "PARTITION BY(FIELD, FIELD2)\n" +
          "LOCALSORT BY(FIELD, FIELD2)\n" +
          "ARROW CACHE true"))
      .runTests();
  }

  @Test
  public void externalReflectionCreateTests() {
    new GoldenFileTestBuilder<>(StatementParserTests::executeTest)
      .add(
        "USING",
        MultiLineString.create("ALTER TABLE EMP CREATE EXTERNAL REFLECTION myReflection\n" +
          "USING"))
      .add(
        "USING + PATH",
        MultiLineString.create("ALTER TABLE EMP CREATE EXTERNAL REFLECTION myReflection\n" +
          "USING \n"+
          "\"External\".EMP"))
      .runTests();
  }

  private static MultiLineString executeTest(MultiLineString text) {
    String queryText = text.toString();
    if (!queryText.contains("^")) {
      queryText = queryText + "^";
    }

    StringAndPos stringAndPos = SqlParserUtil.findPos(queryText);
    Statement statement = Statement.extractCurrentStatementPath(stringAndPos.sql, stringAndPos.cursor).get(0);
    StringBuilder stringBuilder = new StringBuilder();
    StatementSerializer statementSerializer = new StatementSerializer(stringBuilder);
    statement.accept(statementSerializer);

    return MultiLineString.create(stringBuilder.toString());
  }
}
