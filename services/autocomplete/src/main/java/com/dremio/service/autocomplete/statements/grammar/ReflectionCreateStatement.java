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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.AGGREGATE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.CREATE;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.EXTERNAL;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RAW;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.REFLECTION;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.USING;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.tokens.TokenBuffer;

public final class ReflectionCreateStatement {
  private ReflectionCreateStatement() {
  }

  static Statement parse(TokenBuffer tokenBuffer, CatalogPath catalogPath) {
    Preconditions.checkNotNull(tokenBuffer);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(CREATE);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    int kind = tokenBuffer.readKind();
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(REFLECTION);
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    String reflectionName = tokenBuffer.readImage();
    if (tokenBuffer.isEmpty()) {
      return null;
    }

    tokenBuffer.readAndCheckKind(USING);
    switch (kind) {
    case RAW:
      return RawReflectionCreateStatement.parse(tokenBuffer, catalogPath, reflectionName);
    case AGGREGATE:
      return AggregateReflectionCreateStatement.parse(tokenBuffer, catalogPath, reflectionName);
    case EXTERNAL:
      return ExternalReflectionCreateStatement.parse(tokenBuffer, catalogPath, reflectionName);
    default:
      throw new UnsupportedOperationException("UNKNOWN REFLECTION TYPE: " + kind);
    }
  }
}
