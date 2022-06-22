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
import com.google.common.collect.ImmutableList;

/**
 * The JOIN condition inside of a FROM clause.
 *
 * tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER ] ] JOIN tableExpression [ joinCondition ]
 *
 * joinCondition:
 *       ON booleanExpression
 *   |   USING '(' column [, column ]* ')'
 */
public final class JoinCondition {
  private final ImmutableList<DremioToken> tokens;

  private JoinCondition(ImmutableList<DremioToken> tokens) {
    this.tokens = tokens;
  }

  public ImmutableList<DremioToken> getTokens() {
    return tokens;
  }

  public static JoinCondition parse(ImmutableList<DremioToken> tokens) {
    return new JoinCondition(tokens);
  }
}
