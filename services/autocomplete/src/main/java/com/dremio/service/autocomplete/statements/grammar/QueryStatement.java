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

import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Abstract base class for query statements.
 * A QueryStatement can either be a SetQueryStatements joined by SET keywords like (UNION, EXCEPT, MINUS, and INTERSECT)
 * or just a regular SelectQueryStatement that may or may not have nested subqueries.
 */
public abstract class QueryStatement extends Statement {
  private final ImmutableList<QueryStatement> children;

  protected QueryStatement(
    ImmutableList<DremioToken> tokens,
    ImmutableList<QueryStatement> children) {
    super(tokens, children);

    Preconditions.checkNotNull(children);
    this.children = children;
  }

  public ImmutableList<QueryStatement> getChildren() {
    return children;
  }

  public static QueryStatement parse(TokenBuffer tokenBuffer) {
    return SetQueryStatement.parse(tokenBuffer);
  }
}
