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
package com.dremio.dac.service.autocomplete;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.dac.service.autocomplete.utils.DremioToken;
import com.dremio.dac.service.autocomplete.utils.SqlQueryTokenizer;
import com.dremio.dac.service.autocomplete.utils.SqlQueryUntokenizer;
import com.dremio.dac.service.autocomplete.utils.TokenResolver;
import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests used to search the space of valid queries given a certain prefix.
 * This helps us determine which partial queries have a identifier completion and thus need intellisense support.
 */
public final class PrefixedTokensSearchTests {
  private static final DremioToken IDENTIFIER_TOKEN = new DremioToken(ParserImplConstants.IDENTIFIER, "myIdentifier");

  /**
   * This test documents what are the possible top level statements
   * and what work will need to be done to add intellisense support for them.
   */
  @Test
  public void topLevelStatements() {
    GoldenFileTestBuilder.create(PrefixedTokensSearchTests::executeTest)
      .add("EMPTY", new Input("", 1))
      .add("ANALYZE", new Input("ANALYZE", 3))
      .add("CALL", new Input("CALL", 3))
      .add("CASE", new Input("CASE", 1))
      .add("CREATE", new Input("CREATE", 3))
      .add("DELETE", new Input("DELETE", 4))
      .add("DESCRIBE", new Input("DESCRIBE", 2))
      .add("DROP", new Input("DROP", 5))
      .add("EXISTS", new Input("EXISTS", 5))
      .add("EXPLAIN", new Input("EXPLAIN", 3))
      .add("GRANT", new Input("GRANT", 4))
      .add("INSERT", new Input("INSERT", 3))
      .add("MULTISET", new Input("MULTISET", 2))
      .add("NEW", new Input("NEW", 3))
      .add("PERIOD", new Input("PERIOD", 3))
      .add("REFRESH", new Input("REFRESH", 5))
      .add("RESET", new Input("RESET", 3))
      .add("REVOKE", new Input("REVOKE", 5))
      .add("ROLLBACK", new Input("ROLLBACK", 4))
      .add("ROW", new Input("ROW", 3))
      .add("RUNNING", new Input("RUNNING", 3))
      .add("SET", new Input("SET", 3))
      .add("SHOW", new Input("SHOW", 5))
      .add("SPECIFIC", new Input("SPECIFIC", 3))
      .add("TABLE", new Input("TABLE", 3))
      .add("UPSERT", new Input("UPSERT", 3))
      .add("UPDATE", new Input("UPDATE", 5))
      .add("USE", new Input("USE", 4))
      .add("VALUES", new Input("VALUES", 2))
      .add("WITH", new Input("WITH", 3))
      .add("VACUUM", new Input("VACUUM", 5))
      .runTests();
  }

  @Test
  public void alterTable() {
    GoldenFileTestBuilder.create(PrefixedTokensSearchTests::executeTest)
      .add("JUST ALTER TABLE", new Input("ALTER TABLE mytable", 1))
      .add("ALTER TABLE + ALTER", new Input("ALTER TABLE mytable ALTER", 4))
      .add("ALTER TABLE + CHANGE", new Input("ALTER TABLE mytable CHANGE", 4))
      .add("ALTER TABLE + CREATE", new Input("ALTER TABLE mytable CREATE", 4))
      .add("ALTER TABLE + DISABLE", new Input("ALTER TABLE mytable DISABLE", 4))
      .add("ALTER TABLE + DROP", new Input("ALTER TABLE mytable DROP", 4))
      .add("ALTER TABLE + ENABLE", new Input("ALTER TABLE mytable ENABLE", 4))
      .add("ALTER TABLE + FORGET", new Input("ALTER TABLE mytable FORGET", 4))
      .add("ALTER TABLE + LOCALSORT", new Input("ALTER TABLE mytable LOCALSORT", 2))
      .add("ALTER TABLE + MODIFY", new Input("ALTER TABLE mytable MODIFY", 4))
      .add("ALTER TABLE + REFRESH", new Input("ALTER TABLE mytable REFRESH", 5))
      .add("ALTER TABLE + RESET", new Input("ALTER TABLE mytable RESET", 4))
      .add("ALTER TABLE + ROUTE", new Input("ALTER TABLE mytable ROUTE", 4))
      .add("ALTER TABLE + SET", new Input("ALTER TABLE mytable SET", 4))
      .runTests();
  }

  private static final class Input {
    private final String prefix;
    private final int maxDepth;

    public Input(String prefix, int maxDepth) {
      this.prefix = prefix;
      this.maxDepth = maxDepth;
    }

    public String getPrefix() {return prefix;}
    public int getMaxDepth() {return maxDepth;}
  }

  private static final class Output {
    private final List<String> paths;
    private final List<String> pathsWithIdentifierCompletion;

    public Output(List<String> paths, List<String> pathsWithIdentifierCompletion) {
      this.paths = paths;
      this.pathsWithIdentifierCompletion = pathsWithIdentifierCompletion;
    }


    public List<String> getPaths() {
      return paths;
    }

    public List<String> getPathsWithIdentifierCompletion() {
      return pathsWithIdentifierCompletion;
    }
  }

  public static Output executeTest(Input input) {
    String seedPrefix = input.getPrefix();
    int maxDepth = input.getMaxDepth();

    ImmutableList<DremioToken> prefix = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(seedPrefix));
    List<List<DremioToken>> suffixes = new ArrayList<>();
    scanInputTree(
      suffixes,
      prefix,
      ImmutableList.of(),
      maxDepth);

    List<String> paths = new ArrayList<>();
    List<String> pathsWithIdentifierCompletions = new ArrayList<>();

    for (List<DremioToken> suffix : suffixes) {
      String path = SqlQueryUntokenizer.untokenize(prefix) + SqlQueryUntokenizer.untokenize(suffix);
      paths.add(path);

      if (suffix.stream().anyMatch(token -> token.getKind() == ParserImplConstants.IDENTIFIER)) {
        pathsWithIdentifierCompletions.add(path);
      }
    }

    return new Output(paths, pathsWithIdentifierCompletions);
  }

  private static void scanInputTree(
    List<List<DremioToken>> suffixes,
    ImmutableList<DremioToken> prefix,
    ImmutableList<DremioToken> suffix,
    int maxDepth) {
    suffixes.add(suffix);

    ImmutableList<DremioToken> query = ImmutableList.<DremioToken>builder().addAll(prefix).addAll(suffix).build();
    TokenResolver.Predictions predictions = TokenResolver.getNextPossibleTokens(query);
    boolean childless = !predictions.isIdentifierPossible() && predictions.getKeywords().isEmpty();
    boolean hasBudget = maxDepth > 0;
    boolean baseCase = childless || !hasBudget;
    if (baseCase) {
      return;
    }

    maxDepth = maxDepth - 1;
    if (predictions.isIdentifierPossible()) {
      ImmutableList<DremioToken> extendedSuffix = new ImmutableList.Builder<DremioToken>()
        .addAll(suffix)
        .add(IDENTIFIER_TOKEN)
        .build();
      scanInputTree(
        suffixes,
        prefix,
        extendedSuffix,
        maxDepth);
    }

    for (Integer keywordKind : predictions.getKeywords()) {
      if (keywordKind != ParserImplConstants.SEMICOLON) {
        ImmutableList<DremioToken> extendedSuffix = new ImmutableList.Builder<DremioToken>()
          .addAll(suffix)
          .add(DremioToken.createFromParserKind(keywordKind))
          .build();
        scanInputTree(
          suffixes,
          prefix,
          extendedSuffix,
          maxDepth);
      }
    }
  }
}
