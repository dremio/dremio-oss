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

import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.IDENTIFIER;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.LPAREN;
import static com.dremio.exec.planner.sql.parser.impl.ParserImplConstants.RPAREN;

import java.util.Set;

import com.dremio.service.autocomplete.AutocompleteEngineContext;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenBuffer;
import com.google.common.collect.ImmutableList;

/**
 * Represents the fields in a reflection create statement.
 */
public final class FieldList extends LeafStatement {
  private final ImmutableList<DremioToken> tokens;
  private final ImmutableList<String> fields;
  private final TableReference tableReference;

  public FieldList(
    ImmutableList<DremioToken> tokens,
    ImmutableList<String> fields,
    TableReference tableReference) {
    super(tokens);
    this.tokens = tokens;
    this.fields = fields;
    this.tableReference = tableReference;
  }

  public ImmutableList<DremioToken> getTokens() {
    return tokens;
  }

  public TableReference getTableReference() {
    return tableReference;
  }

  public ImmutableList<String> getFields() {
    return fields;
  }

  public static FieldList parse(TokenBuffer tokenBuffer, TableReference tableReference) {
    ImmutableList.Builder<String> fieldBuilder = new ImmutableList.Builder<>();
    ImmutableList.Builder<DremioToken> tokenBuilder = new ImmutableList.Builder<>();
    int level = 0;
    while (!tokenBuffer.isEmpty()) {
      DremioToken token = tokenBuffer.read();
      tokenBuilder.add(token);
      switch (token.getKind()) {
      case IDENTIFIER:
        fieldBuilder.add(token.getImage());
        break;
      case LPAREN:
        level++;
        break;
      case RPAREN:
        level--;
        if (level == 0) {
          return new FieldList(tokenBuilder.build(), fieldBuilder.build(), tableReference);
        }
        break;
      default:
        // Do Nothing
      }
    }

    return new FieldList(tokenBuilder.build(), fieldBuilder.build(), tableReference);
  }

  @Override
  public Completions getCompletions(AutocompleteEngineContext autocompleteEngineContext) {
    Set<ColumnAndTableAlias> allColumns = ColumnResolver
      .create(autocompleteEngineContext)
      .resolve(ImmutableList.of(tableReference));
    ImmutableList<Column> remainingColumns = allColumns
      .stream()
      .filter(columnAndTableAlias -> !fields.contains(columnAndTableAlias.getColumn().getName()))
      .map(ColumnAndTableAlias::getColumn)
      .collect(ImmutableList.toImmutableList());
    return Completions.builder()
      .addColumns(remainingColumns, tableReference)
      .build();
  }
}
