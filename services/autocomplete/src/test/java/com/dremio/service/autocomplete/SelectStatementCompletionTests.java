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
package com.dremio.service.autocomplete;

import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;

public final class SelectStatementCompletionTests extends AutocompleteEngineTests {
  @Test
  public void columnCompletionScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "EMPTY SELECT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT ^ FROM EMP"))
      .add(
        "SINGLE SELECT ITEM",
        GoldenFileTestBuilder.MultiLineString.create("SELECT EMP.ENAME ^  FROM EMP"))
      .add(
        "SINGLE SELECT ITEM AWAITING SECOND SELECT ITEM",
        GoldenFileTestBuilder.MultiLineString.create("SELECT EMP.ENAME, ^  FROM EMP"))
      .add(
        "MIDDLE OF SELECTS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT EMP.ENAME, ^ , EMP.JOB  FROM EMP"))
      .add(
        "ESCAPES IN PATH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"EMP\".\"ENAME\", ^ FROM EMP"))
      .add(
        "SELECT *",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * ^ FROM EMP"))
      .runTests();
  }

  @Test
  public void joinScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "JOIN AWAITING SECOND TABLE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN ^"))
      .add(
        "JOIN AWAITING ON KEYWORD",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ^"))
      .add(
        "JOIN AWAITING EXPRESSION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON ^"))
      .add(
        "JOIN IN MIDDLE OF EXPRESSION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = ^"))
      .add(
        "FROM CLAUSE WITH JOINS",
        GoldenFileTestBuilder.MultiLineString.create("SELECT ^ FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"))
      .add(
        "MULTIPLE JOINS",
        GoldenFileTestBuilder.MultiLineString.create("" +
          "SELECT * FROM EMP " +
          "JOIN DEPT ON EMP.DEPTNO = ^ " +
          "JOIN SALGRADE ON SALGRADE.GRADE = DEPT.DEPTNO"))
      .runTests();
  }

  @Test
  public void pathAliasing() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "BASIC",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP ^"))
      .add(
        "ALIAS ",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AS ^"))
      .add(
        "ALIAS with no as",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP e^"))
      .runTests();
  }

  @Test
  public void pathCompletion() {
    new GoldenFileTestBuilder<>(this::executeTestWithRootContext)
      .add(
        "EMPTY FROM",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM ^"))
      .add(
        "BASIC COMPLETION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".^"))
      .add(
        "COMPLETION WITH A SPACE IN NAME",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space with a space in the name\".^"))
      .add(
        "PATH WITH MANY CHILDREN",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".^"))
      .add(
        "PATH WITH NO CHILDREN",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"file\".^"))
      .add(
        "INVALID PATH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"path\".\"that\".\"does\".\"not\".\"exist\".^"))
      .add(
        "MULTIPLE TABLES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical dataset\", \"space\".\"folder\".^"))
      .add(
        "JOIN empty path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical dataset\" JOIN ^"))
      .add(
        "JOIN mid path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical dataset\" JOIN \"space\".\"folder\".^"))
      .add(
        "APPLY empty path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical dataset\" APPLY ^"))
      .add(
        "APPLY mid path",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"space\".\"folder\".\"physical dataset\" APPLY \"space\".\"folder\".^"))
      .add(
        "Path with special character incorrect.",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM @^"))
      .add(
        "Path with special character correct.",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM \"@^"))
      .runTests();
  }

  @Test
  public void subqueryScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "START OF SUBQUERY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT (^"))
      .add(
        "START OF SUBQUERY WITHOUT SOURCE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM (^"))
      .add("COLUMN in INNER query", GoldenFileTestBuilder.MultiLineString.create(
        "SELECT DEPTNO, (\n" +
          " SELECT MAX(EMP.SAL), ^ \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM DEPT\n"))
      .add("COLUMN in OUTER query", GoldenFileTestBuilder.MultiLineString.create(
        "SELECT DEPTNO, ^, (\n" +
          " SELECT MAX(EMP.SAL) \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM DEPT\n"))
      .add("CATALOGENTRY in INNER query", GoldenFileTestBuilder.MultiLineString.create(
        "SELECT DEPTNO, (\n" +
          " SELECT * \n" +
          " FROM ^" +
          ")\n" +
          "FROM DEPT\n"))
      .add("CATALOGENTRY in OUTER query", GoldenFileTestBuilder.MultiLineString.create(
        "SELECT (\n" +
          " SELECT * \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM ^\n"))
      .runTests();
  }

  @Test
  public void nessie() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "JUST FINISHED TABLE NAME",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP ^"))
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT ^"))
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT B^"))
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT C^"))
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT T^"))
      .add(
        "BRANCH",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT BRANCH ^"))
      .add(
        "COMMIT",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT COMMIT ^"))
      .add(
        "TAG",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP AT TAG ^"))
      .add(
        "Column With Reference",
        GoldenFileTestBuilder.MultiLineString.create("SELECT ^ FROM EMP AT BRANCH \"Branch A\""))
      .add(
        "Set branch",
        GoldenFileTestBuilder.MultiLineString.create("USE BRANCH branchA; SELECT * FROM ^"))
      .runTests();
  }
}
