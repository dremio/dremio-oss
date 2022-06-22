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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.APPLY;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.COMMA;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.CROSS;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FROM;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.FULL;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.JOIN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LEFT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.NATURAL;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.ON;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.OUTER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RIGHT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.USING;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * The FROM clause in a query.
 *
 * FROM tableExpression
 *
 * tableExpression:
 *       tableReference [, tableReference ]*
 *   |   tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER ] ] JOIN tableExpression [ joinCondition ]
 *   |   tableExpression CROSS JOIN tableExpression
 *   |   tableExpression [ CROSS | OUTER ] APPLY tableExpression
 *
 * joinCondition:
 *       ON booleanExpression
 *   |   USING '(' column [, column ]* ')'
 */
public final class FromClause {
  private static final ImmutableSet<Integer> CATALOG_PATH_SEPARATORS = new ImmutableSet.Builder<Integer>()
    .add(COMMA)
    .add(NATURAL)
    .add(LEFT)
    .add(RIGHT)
    .add(FULL)
    .add(OUTER)
    .add(JOIN)
    .add(CROSS)
    .add(APPLY)
    .build();

  private static final ImmutableSet<Integer> JOIN_CONDITION_SEPARATORS = new ImmutableSet.Builder<Integer>()
    .add(ON)
    .add(USING)
    .build();

  private static final ImmutableSet<Integer> SEPARATORS = new ImmutableSet.Builder<Integer>()
    .addAll(CATALOG_PATH_SEPARATORS)
    .addAll(JOIN_CONDITION_SEPARATORS)
    .build();

  private final ImmutableList<DremioToken> tokens;
  private final ImmutableList<CatalogPath> catalogPaths;
  private final ImmutableList<JoinCondition> joinConditions;

  private FromClause(
    ImmutableList<DremioToken> tokens,
    ImmutableList<CatalogPath> catalogPaths,
    ImmutableList<JoinCondition> joinConditions) {
    Preconditions.checkNotNull(tokens);
    Preconditions.checkNotNull(catalogPaths);
    Preconditions.checkNotNull(joinConditions);
    this.tokens = tokens;
    this.catalogPaths = catalogPaths;
    this.joinConditions = joinConditions;
  }

  public ImmutableList<DremioToken> getTokens() {
    return tokens;
  }

  public ImmutableList<CatalogPath> getCatalogPaths() {
    return catalogPaths;
  }

  public ImmutableList<JoinCondition> getJoinConditions() {
    return joinConditions;
  }

  public static FromClause parse(ImmutableList<DremioToken> tokens) {
    return parse(new TokenBuffer(tokens));
  }

  public static FromClause parse(TokenBuffer tokenBuffer) {
    Preconditions.checkNotNull(tokenBuffer);
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    tokenBuffer.readAndCheckKind(FROM);

    ImmutableList.Builder<CatalogPath> tableReferencesBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<JoinCondition> joinConditionsBuilder = new ImmutableList.Builder<>();
    State state = State.IN_CATALOG_PATH;
    while (!tokenBuffer.isEmpty()) {
      ImmutableList<DremioToken> chunk = tokenBuffer.readUntilKinds(SEPARATORS);
      int nextKind = tokenBuffer.readKind();
      switch (state) {
      case IN_CATALOG_PATH:
        CatalogPath catalogPath = CatalogPath.parse(chunk);
        tableReferencesBuilder.add(catalogPath);
        break;

      case IN_JOIN_CONDITION:
        JoinCondition joinCondition = JoinCondition.parse(chunk);
        joinConditionsBuilder.add(joinCondition);
        break;

      default:
        throw new UnsupportedOperationException("UNKNOWN STATE");
      }

      if (nextKind != -1) {
        if (CATALOG_PATH_SEPARATORS.contains(nextKind)) {
          state = State.IN_CATALOG_PATH;
        } else if (JOIN_CONDITION_SEPARATORS.contains(nextKind)) {
          state = State.IN_JOIN_CONDITION;
        } else {
          throw new UnsupportedOperationException("UNKNOWN SEPARATOR");
        }
      }
    }

    return new FromClause(
      tokens,
      tableReferencesBuilder.build(),
      joinConditionsBuilder.build());
  }

  private enum  State {
    IN_CATALOG_PATH,
    IN_JOIN_CONDITION,
  }
}
