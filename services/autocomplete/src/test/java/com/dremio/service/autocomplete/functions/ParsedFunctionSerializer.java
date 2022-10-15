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
package com.dremio.service.autocomplete.functions;

import com.dremio.service.autocomplete.tokens.SqlQueryUntokenizer;
import com.google.common.base.Preconditions;

/**
 * Serialized a ParsedFunction into its parse tree representation.
 */
public final class ParsedFunctionSerializer {
  private ParsedFunctionSerializer() {
  }

  public static String serialize(ParsedFunction parsedFunction) {
    Preconditions.checkNotNull(parsedFunction);

    StringBuilder stringBuilder = new StringBuilder()
      .append("NAME: ")
      .append(parsedFunction.getName())
      .append("\n");

    for (ParsedParameter parsedParameter : parsedFunction.getParameters()) {
      stringBuilder
        .append("PARAMETER: ")
        .append(SqlQueryUntokenizer.untokenize(parsedParameter.getTokens()))
        .append("\n");
    }

    return stringBuilder.toString();
  }
}
