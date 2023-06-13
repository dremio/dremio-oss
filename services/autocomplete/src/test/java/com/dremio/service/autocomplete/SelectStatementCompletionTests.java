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
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "EMPTY SELECT",
        "SELECT ^ FROM EMP")
      .add(
        "SINGLE SELECT ITEM",
        "SELECT EMP.ENAME ^  FROM EMP")
      .add(
        "SINGLE SELECT ITEM AWAITING SECOND SELECT ITEM",
        "SELECT EMP.ENAME, ^  FROM EMP")
      .add(
        "MIDDLE OF SELECTS",
        "SELECT EMP.ENAME, ^ , EMP.JOB  FROM EMP")
      .add(
        "ESCAPES IN PATH",
        "SELECT \"EMP\".\"ENAME\", ^ FROM EMP")
      .add(
        "SELECT *",
        "SELECT * ^ FROM EMP")
      .runTests();
  }

  @Test
  public void joinScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add("JOIN AWAITING SECOND TABLE", "SELECT * FROM EMP JOIN ^")
      .add("JOIN AWAITING ON KEYWORD", "SELECT * FROM EMP JOIN DEPT ^")
      .add("JOIN AWAITING EXPRESSION", "SELECT * FROM EMP JOIN DEPT ON ^")
      .add("JOIN IN MIDDLE OF EXPRESSION", "SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = ^")
      .add("FROM CLAUSE WITH JOINS", "SELECT ^ FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO")
      .add("MULTIPLE JOINS",
          "SELECT * FROM EMP \n" +
            "JOIN DEPT ON EMP.DEPTNO = ^ \n" +
            "JOIN SALGRADE ON SALGRADE.GRADE = DEPT.DEPTNO")
      .runTests();
  }

  @Test
  public void pathAliasing() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "BASIC", "SELECT * FROM EMP ^")
      .add(
        "ALIAS ", "SELECT * FROM EMP AS ^")
      .add(
        "ALIAS with no as", "SELECT * FROM EMP e^")
      .runTests();
  }

  @Test
  public void pathCompletion() {
    new GoldenFileTestBuilder<>(this::executeTestWithRootContext, GoldenFileTestBuilder.MultiLineString::create)
      .add("EMPTY FROM", "SELECT * FROM ^")
      .add("BASIC COMPLETION", "SELECT * FROM \"space\".^")
      .add("COMPLETION WITH A SPACE IN NAME", "SELECT * FROM \"space with a space in the name\".^")
      .add("PATH WITH MANY CHILDREN", "SELECT * FROM \"space\".\"folder\".^")
      .add("PATH WITH NO CHILDREN", "SELECT * FROM \"space\".\"folder\".\"file\".^")
      .add("INVALID PATH", "SELECT * FROM \"path\".\"that\".\"does\".\"not\".\"exist\".^")
      .add("MULTIPLE TABLES", "SELECT * FROM \"space\".\"folder\".\"physical dataset\", \"space\".\"folder\".^")
      .add("JOIN empty path", "SELECT * FROM \"space\".\"folder\".\"physical dataset\" JOIN ^")
      .add("JOIN mid path", "SELECT * FROM \"space\".\"folder\".\"physical dataset\" JOIN \"space\".\"folder\".^")
      .add("APPLY empty path", "SELECT * FROM \"space\".\"folder\".\"physical dataset\" APPLY ^")
      .add("APPLY mid path", "SELECT * FROM \"space\".\"folder\".\"physical dataset\" APPLY \"space\".\"folder\".^")
      .add("Path with special character incorrect.", "SELECT * FROM @^")
      .add("Path with special character correct.", "SELECT * FROM \"@^")
      .runTests();
  }

  @Test
  public void subqueryScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext, GoldenFileTestBuilder.MultiLineString::create)
      .add(
        "START OF SUBQUERY", "SELECT (^")
      .add(
        "START OF SUBQUERY WITHOUT SOURCE", "SELECT * FROM (^")
      .add("COLUMN in INNER query",
        "SELECT DEPTNO, (\n" +
          " SELECT MAX(EMP.SAL), ^ \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM DEPT\n")
      .add("COLUMN in OUTER query",
        "SELECT DEPTNO, ^, (\n" +
          " SELECT MAX(EMP.SAL) \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM DEPT\n")
      .add("CATALOGENTRY in INNER query",
        "SELECT DEPTNO, (\n" +
          " SELECT * \n" +
          " FROM ^" +
          ")\n" +
          "FROM DEPT\n")
      .add("CATALOGENTRY in OUTER query",
        "SELECT (\n" +
          " SELECT * \n" +
          " FROM EMP\n" +
          ")\n" +
          "FROM ^\n")
      .runTests();
  }

  @Test
  public void nessie() {
    GoldenFileTestBuilder.create(this::executeTestWithFolderContext)
      .add(
        "JUST FINISHED TABLE NAME",
        "SELECT * FROM EMP ^")
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        "SELECT * FROM EMP AT ^")
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        "SELECT * FROM EMP AT B^")
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        "SELECT * FROM EMP AT C^")
      .add(
        "JUST FINISHED TABLE NAME WITH AT",
        "SELECT * FROM EMP AT T^")
      .add(
        "BRANCH",
        "SELECT * FROM EMP AT BRANCH ^")
      .add(
        "COMMIT",
        "SELECT * FROM EMP AT COMMIT ^")
      .add(
        "TAG",
        "SELECT * FROM EMP AT TAG ^")
      .add(
        "Column With Reference",
        "SELECT ^ FROM EMP AT BRANCH \"Branch A\"")
      .add(
        "Set branch",
        "USE BRANCH branchA; SELECT * FROM ^")
      .runTests();
  }
}
