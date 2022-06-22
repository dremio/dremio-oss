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

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.statements.visitors.StatementInputOutputVisitor;
import com.dremio.service.autocomplete.statements.visitors.StatementVisitor;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * ALTER DATASET <SOURCE_DATASET_PATH>
 * CREATE EXTERNAL REFLECTION <REFLECTION_NAME>
 * USING <TARGET_DATASET_PATH>
 */
public final class ExternalReflectionCreateStatement extends Statement {
  private final CatalogPath sourcePath;
  private final String name;
  private final CatalogPath targetPath;

  private ExternalReflectionCreateStatement(
    ImmutableList<DremioToken> tokens,
    CatalogPath sourcePath,
    String name,
    CatalogPath targetPath) {
    super(tokens, ImmutableList.of());
    Preconditions.checkNotNull(sourcePath);
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(targetPath);
    this.sourcePath = sourcePath;
    this.name = name;
    this.targetPath = targetPath;
  }

  public CatalogPath getSourcePath() {
    return sourcePath;
  }

  public String getName() {
    return name;
  }

  public CatalogPath getTargetPath() {
    return targetPath;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public <I, O> O accept(StatementInputOutputVisitor<I, O> visitor, I input) {
    return visitor.visit(this, input);
  }

  static ExternalReflectionCreateStatement parse(
    TokenBuffer tokenBuffer,
    CatalogPath sourcePath,
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
        CatalogPath.EMPTY);
    }

    CatalogPath targetPath = CatalogPath.parse(tokens);
    return new ExternalReflectionCreateStatement(
      tokens,
      sourcePath,
      reflectionName,
      targetPath);
  }
}
