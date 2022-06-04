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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.service.autocomplete.catalog.CatalogNodeResolverTests;
import com.dremio.service.autocomplete.catalog.mock.MockSchemas;
import com.dremio.service.autocomplete.columns.ColumnResolverTests;
import com.dremio.service.autocomplete.functions.ParameterResolverTests;
import com.dremio.service.autocomplete.tokens.TokenResolverTests;
import com.dremio.test.GoldenFileTestBuilder;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for the autocomplete engine.
 */
public class AutocompleteEngineTests {
  private static final ImmutableMap<String, ImmutableList<MockSchemas.ColumnSchema>> TABLE_SCHEMAS = new ImmutableMap.Builder<String, ImmutableList<MockSchemas.ColumnSchema>>()
    .put("EMP", MockSchemas.EMP)
    .put("DEPT", MockSchemas.DEPT)
    .put("SALGRADE", MockSchemas.SALGRADE)
    .build();

  public static final AutocompleteEngine AUTOCOMPLETE_ENGINE = new AutocompleteEngine(
    TokenResolverTests.TOKEN_RESOLVER,
    ColumnResolverTests.createColumnResolver(TABLE_SCHEMAS),
    CatalogNodeResolverTests.CATALOG_NODE_RESOLVER,
    ParameterResolverTests.createParameterResolver(ParameterResolverTests.SQL_FUNCTION_DICTIONARY, TABLE_SCHEMAS));

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(AutocompleteEngineTests::executeTest)
      .add(
        "NO FROM CLAUSE",
        MultiLineString.create("SELECT ^"))
      .add(
        "SELECT * FROM",
        MultiLineString.create("SELECT * FROM ^"))
      .add(
        "SELECT * FROM TABLE",
        MultiLineString.create("SELECT * FROM EMP ^"))
      .add(
        "FROM + WHERE",
        MultiLineString.create("SELECT * FROM EMP WHERE ^"))
      .add(
        "CURSOR IN PROJECTION",
        MultiLineString.create("SELECT ^ FROM EMP"))
      .add(
        "FROM CLAUSE WITH COMMAS",
        MultiLineString.create("SELECT ^ FROM EMP, DEPT"))
      .add(
        "FROM CLAUSE WITH JOINS",
        MultiLineString.create("SELECT ^ FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"))
      .add(
        "SUBQUERY WITH CURSOR IN INNER QUERY",
        MultiLineString.create("SELECT DEPTNO, NAME, (SELECT MAX(EMP.SAL), ^\n" +
          " FROM EMP\n" +
          " WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO\n" +
          "FROM DEPT"))
      .add(
        "SUBQUERY WITH CURSOR IN OUTER QUERY",
        MultiLineString.create("SELECT DEPTNO, ^, NAME, (SELECT MAX(EMP.SAL)\n" +
          " FROM EMP\n" +
          " WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO\n" +
          "FROM DEPT"))
      .add(
        "MULTI QUERY",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO, ^ FROM EMP\n" +
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "EVERYTHING",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT DEPTNO, NAME, (SELECT MAX(EMP.SAL), ^\n" +
          " FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO\n" +
          " WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO\n" +
          "FROM DEPT\n" +
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "FUNCTION",
        MultiLineString.create("SELECT ONE_ARG_NUM^"))
      .add(
        "FUNCTION NEEDING PARAMETER",
        MultiLineString.create("SELECT ONE_ARG_NUMERIC_FUNCTION(^ FROM EMP"))
      .runTests();
  }

  private static Output executeTest(MultiLineString query) throws JsonProcessingException {
    final int numCompletions = 5;

    String corpus = query.toString();
    final StringAndPos stringAndPos = SqlParserUtil.findPos(corpus);

    corpus = new StringBuilder(corpus).deleteCharAt(stringAndPos.cursor).toString();

    List<CompletionItem> completions = AUTOCOMPLETE_ENGINE.generateCompletions(
      corpus,
      stringAndPos.cursor);
    List<CompletionItem> filteredCompletions = completions
      .stream()
      .limit(numCompletions)
      .collect(Collectors.toList());
    int numRemaining = Math.max(completions.size() - numCompletions, 0);

    return Output.create(filteredCompletions, numRemaining);
  }

  /**
   * The output for an autocompletion test.
   */
  public static final class Output {
    private final List<MultiLineString> completions;
    private final int numRemaining;

    private Output(List<MultiLineString> completions, int numRemaining) {
      this.completions = completions;
      this.numRemaining = numRemaining;
    }

    public List<MultiLineString> getCompletions() {
      return completions;
    }

    public int getNumRemaining() {
      return numRemaining;
    }

    public static Output create(List<CompletionItem> completionItems, int numRemaining) throws JsonProcessingException {
      ObjectMapper mapper = new ObjectMapper();
      mapper.registerModule(new Jdk8Module().configureAbsentsAsNulls(true));
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

      List<MultiLineString> serializedCompletionItems = new ArrayList<>();

      for (CompletionItem completionItem : completionItems) {
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(completionItem);

        Object deserializedValue = mapper.readValue(json, completionItem.getClass());
        String roundTripJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(deserializedValue);
        Assert.assertEquals(json, roundTripJson);

        serializedCompletionItems.add(MultiLineString.create(json));
      }

      return new Output(serializedCompletionItems, numRemaining);
    }
  }
}

