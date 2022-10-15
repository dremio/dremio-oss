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
public final class SelectQueryStatement extends Statement {
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
    ImmutableList<? extends Statement> children,
    SelectClause selectClause,
    FromClause fromClause,
    UnknownClause remainingClauses) {
    super(tokens,
      new ImmutableList.Builder<Statement>()
        .addAll(children)
        .addAll(asListIgnoringNulls(selectClause, fromClause, remainingClauses))
        .build());

    this.fromClause = fromClause;
  }

  public FromClause getFromClause() {
    return fromClause;
  }

  public static SelectQueryStatement parse(TokenBuffer tokenBuffer) {
    return parseImpl(tokenBuffer, ImmutableList.of());
  }

  private static SelectQueryStatement parseImpl(TokenBuffer tokenBuffer, ImmutableList<TableReference> outerTableReferences) {
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();

    TokenBuffer clauseTokenBuffer = new TokenBuffer(tokens);

    // Parse Clauses
    clauseTokenBuffer.readUntilKind(SELECT);
    ImmutableList<DremioToken> selectClauseTokens = clauseTokenBuffer.readUntilMatchKindAtSameLevel(FROM);
    ImmutableList<DremioToken> fromClauseTokens = clauseTokenBuffer.readUntilMatchKindsAtSameLevel(CLAUSE_KEYWORDS);
    FromClause fromClause = FromClause.parse(fromClauseTokens);
    ImmutableList<TableReference> tableReferences;
    if (fromClause == null) {
      tableReferences = ImmutableList.of();
    } else {
      ImmutableList.Builder<TableReference> tableReferenceBuilder = new ImmutableList.Builder<>();
      for (TableReference tableReference : fromClause.getTableReferences()) {
        if (!tableReference.hasCursor()) {
          tableReferenceBuilder.add(tableReference);
        }
      }

      tableReferences = tableReferenceBuilder.build();
    }

    ImmutableList<TableReference> combinedTableReferences = ImmutableList.<TableReference>builder()
      .addAll(outerTableReferences)
      .addAll(tableReferences)
      .build();
    SelectClause selectClause = SelectClause.parse(new TokenBuffer(selectClauseTokens), combinedTableReferences);
    ImmutableList<DremioToken> remainingClausesTokens = clauseTokenBuffer.drainRemainingTokens();
    UnknownClause remainingClauses = UnknownClause.parse(new TokenBuffer(remainingClausesTokens), combinedTableReferences);

    // Parse Subqueries
    ImmutableList.Builder<SelectQueryStatement> subQueriesBuilder = new ImmutableList.Builder<>();
    while (!tokenBuffer.isEmpty()) {
      tokenBuffer.readUntilKind(LPAREN);
      tokenBuffer.read();
      if (tokenBuffer.kindIs(SELECT)) {
        // Start of a subquery
        ImmutableList<DremioToken> subqueryTokens = tokenBuffer.readUntilMatchKindAtSameLevel(RPAREN);
        tokenBuffer.read();

        SelectQueryStatement subqueryStatement = parseImpl(new TokenBuffer(subqueryTokens), combinedTableReferences);
        subQueriesBuilder.add(subqueryStatement);
      }
    }

    return new SelectQueryStatement(
      tokens,
      subQueriesBuilder.build(),
      selectClause,
      fromClause,
      remainingClauses);
  }
}
