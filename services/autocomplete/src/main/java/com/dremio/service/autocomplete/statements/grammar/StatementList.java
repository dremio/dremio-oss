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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.ALTER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DELETE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.DROP;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SELECT;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.SEMICOLON;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.UPDATE;

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * A semi colon delimited list of Statements:
 *
 * statementList:
 *       statement [ ';' statement ]* [ ';' ]
 */
public final class StatementList extends Statement {
  private StatementList(
    ImmutableList<DremioToken> tokens,
    ImmutableList<Statement> children) {
    super(tokens, children);
  }

  public static StatementList parse(TokenBuffer tokenBuffer) {
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    ImmutableList.Builder<Statement> statementListBuilder = new ImmutableList.Builder<>();
    while (!tokenBuffer.isEmpty()) {
      Statement parsedStatement = parseNext(tokenBuffer);
      statementListBuilder.add(parsedStatement);
      tokenBuffer.read();
    }

    return new StatementList(tokens, statementListBuilder.build());
  }

  private static Statement parseNext(TokenBuffer tokenBuffer) {
    ImmutableList<DremioToken> subTokens = tokenBuffer.readUntilKind(SEMICOLON);
    if (subTokens.isEmpty()) {
      // We could have two consecutive semicolons leading to an empty statement.
      return UnknownStatement.INSTANCE;
    }

    TokenBuffer subBuffer = new TokenBuffer(subTokens);
    int kind = subBuffer.peekKind();
    switch (kind) {
    case SELECT:
      return SetQueryStatement.parse(subBuffer);
    case DROP:
      return DropStatement.parse(subBuffer);
    case DELETE:
      return DeleteStatement.parse(subBuffer);
    case UPDATE:
      return UpdateStatement.parse(subBuffer);
    case ALTER:
      return AlterStatement.parse(subBuffer);
    default:
      return UnknownStatement.parse(subBuffer);
    }
  }
}
