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
import com.google.common.collect.ImmutableList;

/**
 * Baselines to check that we are completing all the different types of functions correctly.
 */
public final class FunctionCompletionTests extends AutocompleteEngineTests {
  @Test
  public void commaSeparated() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "SIMPLE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT ABS(^ FROM EMP"))
      .add(
        "FUNCTION No Source",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE('hello', ^"))
      .add(
        "FUNCTION ONE ARG",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME ^ FROM EMP"))
      .add(
        "FUNCTION ONE ARG + COMMA ",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME, ^ FROM EMP"))
      .add(
        "FUNCTION ONE ARG + COMMA + ONE ARG",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME, 'world'^ FROM EMP"))
      .add(
        "FUNCTION LAST ARG + COMPLETE FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME, EMP.ENAME, ^) FROM EMP"))
      .add(
        "FUNCTION MIDDLE ARG + COMPLETE FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME, ^, EMP.ENAME) FROM EMP"))
      .add(
        "COMPLEX ARG",
        GoldenFileTestBuilder.MultiLineString.create("SELECT REPLACE(EMP.ENAME + EMP.ENAME, ^ FROM EMP"))
      .runTests();
  }

  @Test
  public void substring() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "SUBSTRING PREFIX",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRI^"))
      .add(
        "SUBSTRING SIMPLE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME FROM ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM INTEGER PARAMETER",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME FROM 2 ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM INTEGER PARAMETER FOR",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME FROM 2 FOR ^ FROM EMP"))
      .add(
        "SUBSTRING COMPLETE FUNCTION",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME FROM 2 FOR 3 ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM WITH COMMA",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME, ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM INTEGER PARAMETER WITH COMMA",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME , 2 ^ FROM EMP"))
      .add(
        "SUBSTRING STRING PARAMETER FROM INTEGER PARAMETER FOR WITH COMMA",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME , 2 , ^ FROM EMP"))
      .add(
        "SUBSTRING COMPLETE FUNCTION WITH COMMA",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SUBSTRING(EMP.ENAME , 2 , 3 ^ FROM EMP"))
      .runTests();
  }

  @Test
  public void aggregate() {
    ImmutableList<String> distinctOrAll = ImmutableList.of("ANY_VALUE", "AVG", "BIT_AND", "BIT_OR", "BIT_XOR", "COUNT",
      "COLLECT", "MAX", "MIN", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "SUM", "VAR_POP", "VAR_SAMP", "LISTAGG");
    ImmutableList<String> commaSeparated = ImmutableList.of("COVAR_POP", "COVAR_SAMP", "REGR_SXX",
      "REGR_SYY", "APPROX_COUNT_DISTINCT");
    GoldenFileTestBuilder<GoldenFileTestBuilder.MultiLineString, CompletionsForBaselines> testBuilder = new GoldenFileTestBuilder<>(this::executeTestWithFolderContext);

    for (String function : distinctOrAll) {
      testBuilder
        .add(function + " AGGREGATE PARTIAL NAME",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function.substring(0, function.length() - 1) + "^"))
        .add(function + " AGGREGATE ONLY NAME",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "( ^ FROM EMP"))
        .add(function + " AGGREGATE WITH ALL",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "( ALL ^ FROM EMP" ))
        .add(function + " AGGREGATE WITH DISTINCT",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "( DISTINCT ^ FROM EMP"))
        .add(function + " AGGREGATE WITH DISTINCT VALUE",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "( DISTINCT EMP.ENAME ^ FROM EMP"));
    }

    for (String function : commaSeparated) {
      testBuilder
        .add("COMMA SEPARATED PARTIAL NAME",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function.substring(0, function.length() - 1) + "^"))
        .add("COMMA SEPARATED NAME WITH ONE PARAMETER",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "(EMP.ENAME ^ FROM EMP"))
        .add("COMMA SEPARATED NAME WITH ONE PARAMETER COMMA",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "(EMP.ENAME, ^ FROM EMP"))
        .add("COMMA SEPARATED WITH SECOND PARAMETER",
          GoldenFileTestBuilder.MultiLineString.create("SELECT " + function + "(^ , EMP.ENAME) FROM EMP"));
    }

    testBuilder
      .add("COUNT PARTIAL NAME",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUN^"))
      .add("COUNT NAME ONLY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUNT(^ FROM EMP"))
      .add("COMPLETE COUNT WITH STAR",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUNT( * ^ FROM EMP"))
      .add("COUNT WITH ALL ONE PARAMETER",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUNT( ALL EMP.ENAME ^ FROM EMP"))
      .add("COUNT WITH DISTINCT ONE PARAMETER COMMA",
        GoldenFileTestBuilder.MultiLineString.create("SELECT COUNT( DISTINCT EMP.ENAME, ^ FROM EMP"))
      .add("MODE NAME ONLY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT  MODE(^ FROM EMP"))
      .add("MODE WITH ONE PARAMETER",
        GoldenFileTestBuilder.MultiLineString.create("SELECT  MODE(EMP.ENAME ^ FROM EMP"))
      .runTests();
  }

  @Test
  public void snippetScenarios() {
    new GoldenFileTestBuilder<>(this::executeTestWithFolderContext)
      .add(
        "SUBSTRING FIRST",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE SUBSTRING(^ FROM fromIndex FOR forLength)"))
      .add(
        "SUBSTRING MIDDLE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE SUBSTRING(string FROM ^ FOR forLength)"))
      .add(
        "SUBSTRING END",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE SUBSTRING(string FROM fromIndex FOR ^)"))
      .add(
        "Multiple Argument Function 1",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE BITWISE_AND(^, secondParameter)"))
      .add(
        "Multiple Argument Function 2",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE BITWISE_AND(firstParameter, ^)"))
      .runTests();
  }
}
