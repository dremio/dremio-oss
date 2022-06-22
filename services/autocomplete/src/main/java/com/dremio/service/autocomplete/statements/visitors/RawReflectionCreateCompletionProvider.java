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
package com.dremio.service.autocomplete.statements.visitors;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.statements.grammar.RawReflectionCreateStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.google.common.base.Preconditions;

public final class RawReflectionCreateCompletionProvider {
  private RawReflectionCreateCompletionProvider() {
  }

  public static Completions getCompletionsForIdentifier(
    RawReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(statement);
    Preconditions.checkNotNull(autocompleteEngineContext);
    if ((statement.getDisplayFields() != null) && Cursor.tokensHasCursor(statement.getDisplayFields().getTokens())){
      return getCompletionsForDisplayFields(statement, autocompleteEngineContext);
    }

    if ((statement.getFieldLists().getPartitionFields() != null) && Cursor.tokensHasCursor(statement.getFieldLists().getPartitionFields().getTokens())) {
      return getCompletionsForPartitionFields(statement, autocompleteEngineContext);
    }

    if ((statement.getFieldLists().getDistributeFields() != null) && Cursor.tokensHasCursor(statement.getFieldLists().getDistributeFields().getTokens())) {
      return getCompletionsForDistributeFields(statement, autocompleteEngineContext);
    }

    if ((statement.getFieldLists().getLocalSortFields() != null) && Cursor.tokensHasCursor(statement.getFieldLists().getLocalSortFields().getTokens())) {
      return getCompletionsForLocalSortFields(statement, autocompleteEngineContext);
    }

    throw new UnsupportedOperationException("Cursor was not in any of the expected places for a UPDATE statement.");
  }

  private static Completions getCompletionsForDisplayFields(
    RawReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      statement.getCatalogPath(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForPartitionFields(
    RawReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDisplayFields(),
      statement.getFieldLists().getPartitionFields(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForDistributeFields(
    RawReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDisplayFields(),
      statement.getFieldLists().getDistributeFields(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForLocalSortFields(
    RawReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDisplayFields(),
      statement.getFieldLists().getLocalSortFields(),
      autocompleteEngineContext);
  }
}
