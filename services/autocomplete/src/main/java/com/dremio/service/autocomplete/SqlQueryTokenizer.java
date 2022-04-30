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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.exec.planner.sql.parser.impl.ParserImplTokenManager;
import com.dremio.exec.planner.sql.parser.impl.SimpleCharStream;
import com.dremio.exec.planner.sql.parser.impl.Token;

/**
 * Tokenizer for SQL queries.
 */
public final class SqlQueryTokenizer {
  private SqlQueryTokenizer() {
  }

  public static List<DremioToken> tokenize(String query) {
    assert query != null;

    SimpleCharStream simpleCharStream = new SimpleCharStream(new ByteArrayInputStream(query.getBytes()));
    ParserImplTokenManager tokenManager = new ParserImplTokenManager(simpleCharStream);

    List<DremioToken> tokens = new ArrayList<>();

    while (true) {
      Token token = tokenManager.getNextToken();
      if (token.kind == ParserImplConstants.EOF) {
        break;
      }

      tokens.add(DremioToken.fromCalciteToken(token));
    }

    return tokens;
  }
}
