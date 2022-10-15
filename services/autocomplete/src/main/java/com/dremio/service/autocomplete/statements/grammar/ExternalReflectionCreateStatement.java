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
 * ALTER DATASET <SOURCE_DATASET_PATH>
 * CREATE EXTERNAL REFLECTION <REFLECTION_NAME>
 * USING <TARGET_DATASET_PATH>
 */
public final class ExternalReflectionCreateStatement extends Statement {
  private final TableReference sourcePath;
  private final String name;
  private final TableReference targetPath;

  private ExternalReflectionCreateStatement(
    ImmutableList<DremioToken> tokens,
    TableReference sourcePath,
    String name,
    TableReference targetPath) {
    super(tokens, asListIgnoringNulls(targetPath));
    Preconditions.checkNotNull(sourcePath);
    Preconditions.checkNotNull(name);
    this.sourcePath = sourcePath;
    this.name = name;
    this.targetPath = targetPath;
  }

  public TableReference getSourcePath() {
    return sourcePath;
  }

  public String getName() {
    return name;
  }

  public TableReference getTargetPath() {
    return targetPath;
  }

  static ExternalReflectionCreateStatement parse(
    TokenBuffer tokenBuffer,
    TableReference sourcePath,
    String reflectionName) {
    Preconditions.checkNotNull(tokenBuffer);
    Preconditions.checkNotNull(sourcePath);
    Preconditions.checkNotNull(reflectionName);
    ImmutableList<DremioToken> tokens = tokenBuffer.toList();
    if (tokenBuffer.isEmpty()) {
      return new ExternalReflectionCreateStatement(
        tokens,
        sourcePath,
        reflectionName,
        null);
    }

    TableReference targetPath = TableReference.parse(tokenBuffer);
    return new ExternalReflectionCreateStatement(
      tokens,
      sourcePath,
      reflectionName,
      targetPath);
  }
}
