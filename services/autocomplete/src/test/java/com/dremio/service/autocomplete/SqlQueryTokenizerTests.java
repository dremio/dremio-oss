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

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.SqlQueryTokenizer;
import com.dremio.service.autocomplete.tokens.SqlQueryUntokenizer;
import com.dremio.test.GoldenFileTestBuilder;

public final class SqlQueryTokenizerTests {
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(SqlQueryTokenizerTests::executeTest)
      .add(
        "BASIC QUERY",
        GoldenFileTestBuilder.MultiLineString.create("SELECT * FROM EMP WHERE age < 10 ORDER by age LIMIT 10 OFFSET 10 FETCH FIRST 10 ONLY"))
      .add(
        "IDENTIFIER IN QUOTES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"SOMEIDENTIFIER\" FROM EMP"))
      .add(
        "IDENTIFIER WITH SPECIAL TOKENS ",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"@DREMIO\" FROM EMP"))
      .add(
        "IDENTIFIER WITH SPACES ",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"SOME IDENTIFIER\" FROM EMP"))
      .add(
        "IDENTIFIER WITH UNDERSCORE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT SOME_IDENTIFIER FROM EMP"))
      .add(
        "IDENTIFIER IN QUOTES INCOMPLETE ",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"SOME IDENTIFIER THAT IS NOT COMPLETE"))
      .add(
        "IDENTIFIER WITH INTERSTING SPACE",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"SOME IDENTIFIER\nWITH\tINTERESTING WHITESPACES\" FROM EMP"))
      .add(
        "IDENTIFIER WITH MULTIPLE CONSECUTIVE SPACES",
        GoldenFileTestBuilder.MultiLineString.create("SELECT \"SOME IDENTIFIER  WITH    MULTIPLE      CONSECUTIVE          SPACES\" FROM EMP"))
      .runTests();
  }

  private static Output executeTest(GoldenFileTestBuilder.MultiLineString query) {
    String corpus = query.toString();
    List<DremioToken> tokens = SqlQueryTokenizer.tokenize(corpus);
    String untokenized = SqlQueryUntokenizer.untokenize(tokens);
    return Output.create(tokens, untokenized);
  }

  private static final class Output {
    private final List<String> tokens;
    private final String untokenized;

    private Output(List<String> tokens, String untokenized) {
      this.tokens = tokens;
      this.untokenized = untokenized;
    }

    public List<String> getTokens() {
      return tokens;
    }

    public String getUntokenized() {
      return untokenized;
    }

    public static Output create(List<DremioToken> tokens, String untokenized) {
      return new Output(
        tokens
          .stream()
          .map(token -> token.getImage())
          .collect(Collectors.toList()),
        untokenized);
    }
  }
}
