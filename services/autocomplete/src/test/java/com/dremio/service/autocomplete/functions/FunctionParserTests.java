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
package com.dremio.service.autocomplete.functions;

import java.util.Optional;

import org.junit.Test;

import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.dremio.test.GoldenFileTestBuilder;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.google.common.collect.ImmutableList;

/**
 * Tests that serialize the function parse tree to a baseline file.
 * This is useful for determining if our function parsers are working properly
 */
public final class FunctionParserTests {
  @Test
  public void simple() {
    new GoldenFileTestBuilder<>(FunctionParserTests::executeTest)
      .add("ONLY NAME", "myFunction")
      .add("NAME AND OPEN PARENS", "myFunction(")
      .add("SINGLE PARAMETER", "myFunction(1")
      .add("SINGLE PARAMETER AND COMMA", "myFunction(1, ")
      .add("TWO PARAMETERS", "myFunction(1, 2")
      .add("COMPLETE FUNCTION", "myFunction(1, 2)")
      .add("COMPLETE FUNCTION WITH TRAILING PARAMETERS", "myFunction(1, 2) this text is garbage and should not be parsed")
      .add("COMPLEX PARAMETER", "myFunction(1 + 1, ")
      .add("COMPLEX PARAMETER 2", "myFunction(1 + 1, 3")
      .add("SUBFUNCTION", "myFunction(subFunction(1, 2, 3), ")
      .add("SUBFUNCTION 2", "myFunction(subFunction(1, 2, 3), 4")
      .add("SUBFUNCTION 3", "myFunction(subFunction(1, 2, 3), 4)")
      .runTests();
  }

  @Test
  public void substring() {
    new GoldenFileTestBuilder<>(FunctionParserTests::executeTest)
      .add("ONLY NAME", "SUBSTRING")
      .add("NAME AND OPEN PARENS", "SUBSTRING(")
      .add("SINGLE STRING PARAMETER", "SUBSTRING('hello'")
      .add("SINGLE STRING PARAMETER AND FROM", "SUBSTRING('hello' FROM ")
      .add("STRING PARAMETER FROM INTEGER PARAMETER", "SUBSTRING('hello' FROM 2")
      .add("COMPLETE FUNCTION", "SUBSTRING('hello' FROM 2)")
      .add("COMPLETE FUNCTION DIFFERENT CASE", "SubString('hello' FROM 2)")
      .add("STRING PARAMETER FROM INTEGER PARAMETER FOR", "SUBSTRING('hello' FROM 2 FOR")
      .add("FUNCTION WITH FOR AND OPEN PARENS", "SUBSTRING('hello' FROM 2 FOR 3")
      .add("COMPLETE FUNCTION WITH FOR", "SUBSTRING('hello' FROM 2 FOR 3)") // Consider case where comma is used
      .add("INVALID STRUCTURE", "SUBSTRING('hello' 2 ")
      .add("NESTED FUNCTION", "SUBSTRING(SUBSTRING('hello' FROM 2 FOR 3) FROM 0 ")
      .add("STRING PARAMETER WITH COMMA", "SUBSTRING('hello' , ")
      .add("STRING PARAMETER FROM INTEGER PARAMETER WITH COMMA", "SUBSTRING('hello' , 2")
      .add("COMPLETE FUNCTION WITH COMMA", "SUBSTRING('hello' , 2)")
      .add("STRING PARAMETER FROM INTEGER PARAMETER FOR WITH COMMA", "SUBSTRING('hello' , 2 ,")
      .add("FUNCTION WITH FOR AND OPEN PARENS WITH COMMA", "SUBSTRING('hello' , 2 , 3")
      .add("COMPLETE FUNCTION WITH FOR WITH COMMA", "SUBSTRING('hello' , 2 , 3)")
      .runTests();
  }

  @Test
  public void aggregate() {
    ImmutableList<String> distinctOrAll = ImmutableList.of("ANY_VALUE", "AVG", "BIT_AND", "BIT_OR", "BIT_XOR", "COUNT",
      "COLLECT", "MAX", "MIN", "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "SUM", "VAR_POP", "VAR_SAMP", "LISTAGG");
    ImmutableList<String> commaSeparated = ImmutableList.of("COVAR_POP", "COVAR_SAMP", "REGR_COUNT", "REGR_SXX",
      "REGR_SYY", "APPROX_COUNT_DISTINCT");
    ImmutableList<String> conditioned = ImmutableList.of("EVERY", "SOME");
    ImmutableList<String> multisetFunctions = ImmutableList.of("FUSION", "INTERSECTION");
    GoldenFileTestBuilder<String, MultiLineString> testBuilder = new GoldenFileTestBuilder<>(FunctionParserTests::executeTest);

    for (String function : distinctOrAll) {
      testBuilder
        .add("ONLY NAME", function)
        .add("NAME WITH ALL", function + "(ALL ")
        .add("NAME WITH DISTINCT", function + "(DISTINCT ")
        .add("NAME WITH DISTINCT VALUE", function + "(DISTINCT EMP.NUMBER ")
        .add("NAME WITH ALL VALUE", function + "(ALL EMP.NUMBER ")
        .add("NAME WITH VALUE", function + "(EMP.NUMBER")
        .add("COMPLETE FUNCTION WITH DISTINCT", function + "(DISTINCT EMP.NUMBER)")
        .add("COMPLETE FUNCTION WITH ALL", function + "(ALL EMP.NUMBER)")
        .add("COMPLETE FUNCTION WITH VALUE", function + "(EMP.NUMBER)");
    }

    for (String function : commaSeparated) {
      testBuilder
        .add("COMMA SEPARATED NAME WITH ONE PARAMETER", function + "(EMP.NUMERIC1")
        .add("COMMA SEPARATED NAME WITH ONE PARAMETER COMMA", function + "(EMP.NUMERIC1, ")
        .add("COMMA SEPARATED NAME WITH TWO PARAMETERS", function + "(EMP.NUMERIC1, EMP.NUMERIC2")
        .add("COMMA SEPARATED COMPLETE FUNCTION", function + "(EMP.NUMERIC1, EMP.NUMERIC2");
    }

    for(String function : conditioned) {
      testBuilder
        .add("CONDITIONED INCOMPLETE CONDITION", function + "(EMP.NUMBER")
        .add("CONDITIONED WITH CONDITION", function + "(EMP.NUMBER < 0");
    }

    for(String function : multisetFunctions) {
      testBuilder
        .add("MULTISET WITH INCOMPLETE PARAMETER",   function + "(MULTISET(SELECT NAME ")
        .add("MULTISET WITH PARAMETER",  function + "(MULTISET(SELECT NAME FROM EMP)")
        .add("MULTISET COMPLETE FUNCTION",  function + "(MULTISET(SELECT NAME FROM EMP))");
    }

    testBuilder
      .add("COUNT WITH STAR", "COUNT(*" )
      .add("COMPLETE COUNT WITH STAR", "COUNT(* )" )
      .add("COUNT WITH ALL MULTIPLE PARAMETERS", "COUNT(ALL EMP.NUMBER, EMP.NAME, EMP.OFFICE")
      .add("COUNT WITH DISTINCT MULTIPLE PARAMETERS", "COUNT(DISTINCT EMP.NUMBER, EMP.NAME, EMP.OFFICE")
      .add("MODE NAME ONLY", "MODE( ")
      .add("MODE WITH ONE PARAMETER", "MODE( EMP.NUMBER")
      .add("MODE COMPLETE", "MODE( EMP.NUMBER )")
      .runTests();
  }

  private static MultiLineString executeTest(String corpus) {
    TokenBuffer tokenBuffer = TokenBuffer.create(corpus);
    Optional<ParsedFunction> optionalParsedFunction = ParsedFunction.tryParse(tokenBuffer);
    if (!optionalParsedFunction.isPresent()) {
      return MultiLineString.create("FAILED TO PARSE");
    }

    return MultiLineString.create(ParsedFunctionSerializer.serialize(optionalParsedFunction.get()));
  }
}
