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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.completions.CompletionItem;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.statements.grammar.CatalogPath;
import com.dremio.service.autocomplete.statements.grammar.FieldList;
import com.dremio.service.autocomplete.statements.grammar.FromClause;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

public final class Utils {
  private static final DremioToken FROM_TOKEN = DremioToken.createFromParserKind(ParserImplConstants.FROM);

  public static Completions getColumns(
    CatalogPath catalogPath,
    AutocompleteEngineContext autocompleteEngineContext) {
    ImmutableList<DremioToken> fromClauseTokens = ImmutableList.<DremioToken>builder()
      .add(FROM_TOKEN)
      .addAll(catalogPath.getTokens())
      .build();
    FromClause fromClause = FromClause.parse(new TokenBuffer(fromClauseTokens));
    Set<ColumnAndTableAlias> columnAndTableAliases = ColumnResolver
      .create(autocompleteEngineContext)
      .resolve(fromClause);
    return Completions
      .builder()
      .addColumnAndTableAliases(ImmutableList.copyOf(columnAndTableAliases), null)
      .build();
  }

  public static Completions getCatalogEntries(
    CatalogPath catalogPath,
    AutocompleteEngineContext autocompleteEngineContext) {
    return Completions
      .builder()
      .addCatalogNodes(autocompleteEngineContext
        .getAutocompleteSchemaProvider()
        .getChildrenInScope(catalogPath.getPathTokens()))
      .build();
  }

  public static Completions getFields(
    CatalogPath catalogPath,
    FieldList displayFields,
    FieldList otherFields,
    AutocompleteEngineContext autocompleteEngineContext) {
    List<CompletionItem> allColumns = getColumns(catalogPath, autocompleteEngineContext).getCompletionItems();
    Set<String> displayFieldsSet = new HashSet<>(displayFields
      .getFields()
      .stream()
      .map(field -> field.toUpperCase())
      .collect(Collectors.toList()));
    Set<String> otherFieldsSet = new HashSet<>(otherFields
      .getFields()
      .stream()
      .map(field -> field.toUpperCase())
      .collect(Collectors.toList()));
    Set<String> unusedFields = com.google.common.collect.Sets.difference(displayFieldsSet, otherFieldsSet);
    ImmutableList<CompletionItem> unusedColumns = allColumns
      .stream()
      .filter(column -> unusedFields.contains(column.getLabel().toUpperCase()))
      .collect(ImmutableList.toImmutableList());
    return Completions.builder().addItems(unusedColumns).build();
  }
}
