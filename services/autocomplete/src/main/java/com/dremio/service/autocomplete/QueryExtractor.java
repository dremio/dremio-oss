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

import java.util.Optional;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.google.common.collect.ImmutableList;

/**
 * Extracts out portions of a query from a raw corpus.
 */
public final class QueryExtractor {
  private static final ImmutableList<Integer> QUERY_SEPARATORS = ImmutableList.<Integer>builder()
    .add(ParserImplConstants.UNION)
    .add(ParserImplConstants.EXCEPT)
    .add(ParserImplConstants.SET_MINUS)
    .add(ParserImplConstants.INTERSECT)
    .build();
  private static final ImmutableList<Integer> CLAUSE_KEYWORDS = ImmutableList.<Integer>builder()
    .add(ParserImplConstants.WHERE)
    .add(ParserImplConstants.ORDER)
    .add(ParserImplConstants.LIMIT)
    .add(ParserImplConstants.OFFSET)
    .add(ParserImplConstants.FETCH)
    .build();

  private QueryExtractor() {}

  public static TokenRanges extractTokenRanges(
    final ImmutableList<DremioToken> multiQueryTokens,
    final int multiQueryTokensCursorIndex) {
    final TokenRange queryTokenRange = extractQueryLocation(
      multiQueryTokens,
      multiQueryTokensCursorIndex);

    final ImmutableList<DremioToken> queryTokens = multiQueryTokens.subList(
      queryTokenRange.getStartIndexInclusive(),
      queryTokenRange.getEndIndexExclusive());
    final Integer queryTokensCursorIndex = multiQueryTokensCursorIndex - queryTokenRange.getStartIndexInclusive();

    final TokenRange localSubqueryTokenRange = extractSubqueryLocation(
      queryTokens,
      queryTokensCursorIndex);
    final ImmutableList<DremioToken> subqueryTokens = queryTokens.subList(
      localSubqueryTokenRange.getStartIndexInclusive(),
      localSubqueryTokenRange.getEndIndexExclusive());
    final TokenRange globalSubqueryTokenRange = new TokenRange(
      queryTokenRange.getStartIndexInclusive() + localSubqueryTokenRange.getStartIndexInclusive(),
      queryTokenRange.getStartIndexInclusive() + localSubqueryTokenRange.getEndIndexExclusive());

    final TokenRange localFromClauseTokenRange = extractFromClauseLocation(
      subqueryTokens);
    final TokenRange globalFromClauseTokenRange = new TokenRange(
      globalSubqueryTokenRange.getStartIndexInclusive() + localFromClauseTokenRange.getStartIndexInclusive(),
      globalSubqueryTokenRange.getStartIndexInclusive() + localFromClauseTokenRange.getEndIndexExclusive());

    return new TokenRanges(
      queryTokenRange,
      globalSubqueryTokenRange,
      globalFromClauseTokenRange);
  }

  /**
   * Extracts a query from a larger query separated by keywords like "UNION", "EXCEPT", "MINUS", and "INTERSECT".
   * Ex:
   * SELECT * FROM EMP
   * UNION
   * SELECT ^ FROM DEPT
   * UNION
   * SELECT * FROM SALGRADE
   *
   * would return:
   * SELECT ^ FROM DEPT
   *
   * since that is where the cursor is.
   * @param tokens
   * @return
   */
  private static TokenRange extractQueryLocation(
    final ImmutableList<DremioToken> tokens,
    final int cursorIndex) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkArgument(cursorIndex >= 0, "cursor index must be non negative.");

    /*
    The grammar for a query looks like this:
    query:
          values
      |   WITH withItem [ , withItem ]* query
      |   {
              select
          |   selectWithoutFrom
          |   query UNION [ ALL | DISTINCT ] query
          |   query EXCEPT [ ALL | DISTINCT ] query
          |   query MINUS [ ALL | DISTINCT ] query
          |   query INTERSECT [ ALL | DISTINCT ] query
          }
          [ ORDER BY orderItem [, orderItem ]* ]
          [ LIMIT [ start, ] { count | ALL } ]
          [ OFFSET start { ROW | ROWS } ]
          [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]
     */

    final ImmutableList<DremioToken> subTokens = tokens.subList(0, cursorIndex);
    final int querySeparatorBeforeCursorIndex = QUERY_SEPARATORS
      .stream()
      .map(querySeparator -> Utils.lastIndexOf(subTokens, querySeparator))
      .reduce(Optional.of(0), (maxSoFar, next) -> Utils.max(maxSoFar, next))
      .get();

    final ImmutableList<DremioToken> beforeCursorSubTokens = tokens.subList(querySeparatorBeforeCursorIndex, subTokens.size());
    final int startQueryIndex = Utils.indexOf(
      beforeCursorSubTokens,
      ParserImplConstants.SELECT)
      .get() + querySeparatorBeforeCursorIndex;

    final ImmutableList<DremioToken> fromStartQuerySubTokens = tokens.subList(startQueryIndex, tokens.size());
    final int endQueryIndex = QUERY_SEPARATORS
      .stream()
      .map(querySeparator -> Utils.indexOf(fromStartQuerySubTokens, querySeparator))
      .reduce(Optional.of(fromStartQuerySubTokens.size()), (minSoFar, next) -> Utils.min(minSoFar, next))
      .get() + startQueryIndex;

    return new TokenRange(startQueryIndex, endQueryIndex);
  }

  /**
   * Extracts out a query from a subquery based on the cursor (or leaves it as is if the cursor is on the outer query).
   *
   * For example:
   * SELECT DEPTNO, NAME, (SELECT MAX(EMP.SAL), ^
   *  FROM EMP
   *  WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO
   * FROM DEPT
   *
   * returns:
   * (SELECT MAX(EMP.SAL), ^
   *  FROM EMP
   *  WHERE EMP.DEPTNO = DEPT.DEPTNO)
   *  since the cursor is in the inner query
   *
   * and:
   * SELECT DEPTNO, NAME, ^, (SELECT MAX(EMP.SAL),
   *  FROM EMP
   *  WHERE EMP.DEPTNO = DEPT.DEPTNO) AS MAX_SAL_IN_DEPTNO
   * FROM DEPT
   *
   * just returns the query as is, since the cursor is on the outer query
   * @param tokens
   * @return
   */
  private static TokenRange extractSubqueryLocation(
    ImmutableList<DremioToken> tokens,
    int cursorIndex) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkArgument(cursorIndex >= 0, "cursor index must be non negative.");

    Optional<Integer> subQueryStartIndex = Optional.empty();
    for (int index = 0; index < cursorIndex - 1; index++) {
      DremioToken firstToken = tokens.get(index);
      if (firstToken.getKind() == ParserImplConstants.LPAREN) {
        DremioToken secondToken = tokens.get(index + 1);
        if (secondToken.getKind() == ParserImplConstants.SELECT) {
          subQueryStartIndex = Optional.of(index);
        }
      }
    }

    if (!subQueryStartIndex.isPresent()) {
      // No subquery so just return the corpus as is:
      return new TokenRange(0, tokens.size());
    }

    // We are inside of a subquery and need to extract it out
    int parensCounter = 1;
    int subQueryStartEndIndex = subQueryStartIndex.get() + 1;
    while ((parensCounter != 0) && (subQueryStartEndIndex != tokens.size())) {
      switch (tokens.get(subQueryStartEndIndex).getKind()) {
      case ParserImplConstants.LPAREN:
        parensCounter++;
        break;
      case ParserImplConstants.RPAREN:
        parensCounter--;
        break;
      default:
        // Do Nothing
      }

      subQueryStartEndIndex++;
    }

    return new TokenRange(subQueryStartIndex.get(), subQueryStartEndIndex);
  }

  private static TokenRange extractFromClauseLocation(
    ImmutableList<DremioToken> tokens) {
    Preconditions.checkNotNull(tokens);

    final Optional<Integer> startOfFromClauseIndex = Utils.lastIndexOf(
      tokens,
      ParserImplConstants.FROM);
    if (!startOfFromClauseIndex.isPresent()) {
      throw new UnsupportedOperationException("corpus does not have a FROM clause");
    }

    final ImmutableList<DremioToken> subTokens = tokens.subList(startOfFromClauseIndex.get(), tokens.size());
    final int endOfFromClauseIndex = CLAUSE_KEYWORDS
      .stream()
      .map(clauseKeyword -> Utils.indexOf(subTokens, clauseKeyword))
      .reduce(Optional.of(subTokens.size()), (minSoFar, nextElement) -> Utils.min(minSoFar, nextElement))
      .get() + startOfFromClauseIndex.get();

    return new TokenRange(startOfFromClauseIndex.get(), endOfFromClauseIndex);
  }

  /**
   * The range of tokens that match the extracted query context.
   */
  public static final class TokenRange {
    private final int startIndexInclusive;
    private final int endIndexExclusive;

    public TokenRange(int startIndexInclusive, int endIndexExclusive) {
      this.startIndexInclusive = startIndexInclusive;
      this.endIndexExclusive = endIndexExclusive;
    }

    public int getStartIndexInclusive() {
      return startIndexInclusive;
    }

    public int getEndIndexExclusive() {
      return endIndexExclusive;
    }
  }

  /**
   * Ranges for all query contexts.
   */
  public static final class TokenRanges {
    private final TokenRange query;
    private final TokenRange subquery;
    private final TokenRange fromClause;

    public TokenRanges(TokenRange query, TokenRange subquery, TokenRange fromClause) {
      this.query = query;
      this.subquery = subquery;
      this.fromClause = fromClause;
    }

    public TokenRange getQuery() {
      return query;
    }

    public TokenRange getSubquery() {
      return subquery;
    }

    public TokenRange getFromClause() {
      return fromClause;
    }
  }
}
