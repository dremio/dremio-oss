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
package com.dremio.dac.service.autocomplete.utils;

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;

import java.util.List;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;

/**
 * Takes a list of DremioTokens and serializes it back into a string.
 */
public final class SqlQueryUntokenizer {
  private SqlQueryUntokenizer() {
  }

  public static String untokenize(List<DremioToken> tokens) {
    StringBuilder stringBuilder = new StringBuilder();
    for (DremioToken token : tokens) {
      if (token.getKind() == ParserImplConstants.IDENTIFIER) {
        String escapedToken = quoteIdentifier(token.getImage());
        stringBuilder.append(escapedToken);
      } else {
        stringBuilder.append(token.getImage());
      }
      stringBuilder.append(" ");
    }

    return stringBuilder.toString();
  }
}
