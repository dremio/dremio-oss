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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FETCH;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FROM;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.GROUP;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.HAVING;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LIMIT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.OFFSET;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.ORDER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SELECT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.WHERE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.WINDOW;

import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * A Query statement, which may or may not have nested subqueries:
 *
 * SELECT [ hintComment ] [ STREAM ] [ ALL | DISTINCT ]
 *    { * | projectItem [, projectItem ]* }
 * FROM tableExpression
 * [ WHERE booleanExpression ]
 * [ GROUP BY { groupItem [, groupItem ]* } ]
 * [ HAVING booleanExpression ]
 * [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]
 *
 */
public final class SelectQueryStatement extends QueryStatement {
  private static final DremioToken FROM_TOKEN = DremioToken.createFromParserKind(FROM);
  private static final ImmutableSet<Integer> CLAUSE_KEYWORDS = ImmutableSet.<Integer>builder()
    .add(WHERE)
    .add(ORDER)
    .add(LIMIT)
    .add(OFFSET)
    .add(FETCH)
    .add(GROUP)
    .add(HAVING)
    .add(WINDOW)
    .build();

  private final FromClause fromClause;

  private SelectQueryStatement(
    ImmutableList<DremioToken> tokens,
    ImmutableList<QueryStatement> children,
    FromClause fromClause) {
    super(tokens, children);

    this.fromClause = fromClause;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public <I, O> O accept(StatementInputOutputVisitor<I, O> visitor, I input) {
    return visitor.visit(this, input);
  }

  public FromClause getFromClause() {
    return fromClause;
  }

  public static QueryStatement parse(TokenBuffer tokenBuffer) {
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    ImmutableList.Builder<QueryStatement> subQueriesBuilder = new ImmutableList.Builder<>();
    FromClause fromClause = null;
    while (!tokenBuffer.isEmpty()) {
      tokenBuffer.readUntilKind(LPAREN);
      tokenBuffer.read();
      if (tokenBuffer.kindIs(SELECT)) {
        // Start of a subquery
        ImmutableList<DremioToken> subqueryTokens = tokenBuffer.readUntilMatchKindAtSameLevel(RPAREN);
        tokenBuffer.read();
        QueryStatement subqueryStatement = parse(new TokenBuffer(subqueryTokens));
        subQueriesBuilder.add(subqueryStatement);
      }
    }

    ImmutableList<DremioToken> fromClauseTokens = extractOutFromClauseTokens(new TokenBuffer(tokens));
    if (fromClauseTokens != null) {
      fromClause = FromClause.parse(new TokenBuffer(fromClauseTokens));
    }

    return new SelectQueryStatement(
      tokens,
      subQueriesBuilder.build(),
      fromClause);
  }

  private static ImmutableList<DremioToken> extractOutFromClauseTokens(TokenBuffer tokenBuffer) {
    tokenBuffer.readUntilKind(SELECT);
    tokenBuffer.read();
    tokenBuffer.readUntilMatchKindAtSameLevel(FROM);

    if (tokenBuffer.isEmpty()) {
      return null;
    }

    return tokenBuffer.readUntilMatchKindsAtSameLevel(CLAUSE_KEYWORDS);
  }
}
