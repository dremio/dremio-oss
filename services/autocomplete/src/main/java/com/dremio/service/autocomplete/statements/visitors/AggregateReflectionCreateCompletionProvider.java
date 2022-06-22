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
import com.dremio.service.autocomplete.statements.grammar.AggregateReflectionCreateStatement;
import com.dremio.service.autocomplete.tokens.Cursor;
import com.google.common.base.Preconditions;

public final class AggregateReflectionCreateCompletionProvider {
  private AggregateReflectionCreateCompletionProvider() {}

  public static Completions getCompletionsForIdentifier(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    Preconditions.checkNotNull(statement);
    Preconditions.checkNotNull(autocompleteEngineContext);

    if ((statement.getDimensions() != null) && Cursor.tokensHasCursor(statement.getDimensions().getTokens())){
      return getCompletionsForDimensions(statement, autocompleteEngineContext);
    }

    if ((statement.getMeasures() != null) && Cursor.tokensHasCursor(statement.getMeasures().getTokens())) {
      return getCompletionsForMeasures(statement, autocompleteEngineContext);
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

  private static Completions getCompletionsForDimensions(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      statement.getCatalogPath(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForMeasures(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getColumns(
      statement.getCatalogPath(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForPartitionFields(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDimensions(),
      statement.getFieldLists().getPartitionFields(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForDistributeFields(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDimensions(),
      statement.getFieldLists().getDistributeFields(),
      autocompleteEngineContext);
  }

  private static Completions getCompletionsForLocalSortFields(
    AggregateReflectionCreateStatement statement,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Utils.getFields(
      statement.getCatalogPath(),
      statement.getDimensions(),
      statement.getFieldLists().getLocalSortFields(),
      autocompleteEngineContext);
  }
}
