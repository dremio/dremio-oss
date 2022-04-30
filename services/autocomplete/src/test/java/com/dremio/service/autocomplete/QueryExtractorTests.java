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

import static com.dremio.test.GoldenFileTestBuilder.*;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.util.Pair;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests for QueryExtractor.
 */
public final class QueryExtractorTests {
  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(QueryExtractorTests::executeTest)
      .add(
        "ONLY FROM CLAUSE",
        MultiLineString.create("SELECT * FROM EMP^"))
      .add(
        "FROM + WHERE",
        MultiLineString.create("SELECT * FROM EMP WHERE age < 10^"))
      .add(
        "FROM + ORDER BY",
        MultiLineString.create("SELECT * FROM EMP ORDER BY age^"))
      .add(
        "FROM + LIMIT",
        MultiLineString.create("SELECT * FROM EMP LIMIT 10^"))
      .add(
        "FROM + OFFSET",
        MultiLineString.create("SELECT * FROM EMP OFFSET 10^"))
      .add(
        "FROM + FETCH",
        MultiLineString.create("SELECT * FROM EMP FETCH FIRST 10 ONLY^"))
      .add(
        "MULTIPLE CLAUSES",
        MultiLineString.create("SELECT * FROM EMP WHERE age < 10 ORDER by age LIMIT 10 OFFSET 10 FETCH FIRST 10 ONLY^"))
      .add(
        "FROM CLAUSE WITH COMMAS",
        MultiLineString.create("SELECT * FROM EMP, DEPT^"))
      .add(
        "FROM CLAUSE WITH JOINS",
        MultiLineString.create("SELECT * FROM EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO^"))
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
        "MULTI QUERY (UNION)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "MULTI QUERY (EXCEPT)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " EXCEPT\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "MULTI QUERY (MINUS)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " MINUS\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "MULTI QUERY (INTERSECT)",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " INTERSECT\n" +
          "SELECT EMPNO FROM EMP"))
      .add(
        "MULTI QUERY FIRST",
        MultiLineString.create("SELECT DEPTNO, ^ FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP\n"+
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "MULTI QUERY MIDDLE",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO, ^ FROM EMP\n"+
          " UNION\n" +
          "SELECT GRADE FROM SALGRADE"))
      .add(
        "MULTI QUERY LAST",
        MultiLineString.create("SELECT DEPTNO FROM DEPT\n" +
          " UNION\n" +
          "SELECT EMPNO FROM EMP\n"+
          " UNION\n" +
          "SELECT GRADE, ^ FROM SALGRADE"))
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
      .runTests();
  }

  private static Output executeTest(MultiLineString query) {
    String corpus = query.toString();
    final StringAndPos stringAndPos = SqlParserUtil.findPos(corpus);

    corpus = new StringBuilder(corpus).deleteCharAt(stringAndPos.cursor).toString();

    final Pair<ImmutableList<DremioToken>, Integer> tokensAndIndex = tokenizeAndGetCursorIndex(
      corpus,
      stringAndPos.cursor);

    final QueryExtractor.TokenRanges tokenRanges = QueryExtractor.extractTokenRanges(
      tokensAndIndex.left,
      tokensAndIndex.right);
    final ImmutableList<DremioToken> queryTokens = tokensAndIndex.left.subList(
      tokenRanges.getQuery().getStartIndexInclusive(),
      tokenRanges.getQuery().getEndIndexExclusive());
    final ImmutableList<DremioToken> subQueryTokens = tokensAndIndex.left.subList(
      tokenRanges.getSubquery().getStartIndexInclusive(),
      tokenRanges.getSubquery().getEndIndexExclusive());
    final ImmutableList<DremioToken> fromClauseTokens = tokensAndIndex.left.subList(
      tokenRanges.getFromClause().getStartIndexInclusive(),
      tokenRanges.getFromClause().getEndIndexExclusive());

    final String extractedQuery = SqlQueryUntokenizer.untokenize(queryTokens);
    final String extractedSubquery = SqlQueryUntokenizer.untokenize(subQueryTokens);
    final String extractedFromClause = SqlQueryUntokenizer.untokenize(fromClauseTokens);

    return Output.create(
      extractedQuery,
      extractedSubquery,
      extractedFromClause);
  }

  private static Pair<ImmutableList<DremioToken>, Integer> tokenizeAndGetCursorIndex(
    final String corpus,
    final int cursorIndex) {
    Preconditions.checkNotNull(corpus);
    Preconditions.checkArgument(cursorIndex >= 0);

    final ImmutableList<DremioToken> tokens = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(corpus));
    // Remove whitespace from cursorIndex count
    final int whitespaceCount = (int) corpus.substring(0, cursorIndex).chars().filter(Character::isWhitespace).count();
    final int cursorIndexWithoutWhitespace = cursorIndex - whitespaceCount;
    int tokenCursorIndex = 0;
    for (int charactersRead = 0; (tokenCursorIndex < tokens.size()) && (charactersRead < cursorIndexWithoutWhitespace); tokenCursorIndex++) {
      DremioToken token = tokens.get(tokenCursorIndex);
      charactersRead += token.getImage().length();
    }

    return new Pair<>(tokens, tokenCursorIndex);
  }

  private static final class Output {
    private final MultiLineString extractedQuery;
    private final MultiLineString extractedSubquery;
    private final MultiLineString extractedFromClause;

    private Output(
      MultiLineString extractedQuery,
      MultiLineString extractedSubquery,
      MultiLineString extractedFromClause) {
      this.extractedQuery = extractedQuery;
      this.extractedSubquery = extractedSubquery;
      this.extractedFromClause = extractedFromClause;
    }

    public static Output create(
      String extractedQuery,
      String extractedSubquery,
      String extractedFromClause) {
      return new Output(
        MultiLineString.create(extractedQuery),
        MultiLineString.create(extractedSubquery),
        MultiLineString.create(extractedFromClause));
    }

    public MultiLineString getExtractedQuery() {
      return extractedQuery;
    }

    public MultiLineString getExtractedSubquery() {
      return extractedSubquery;
    }

    public MultiLineString getExtractedFromClause() {
      return extractedFromClause;
    }
  }
}
