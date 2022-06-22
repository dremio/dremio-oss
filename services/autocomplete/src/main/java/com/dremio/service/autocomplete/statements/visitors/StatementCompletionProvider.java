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
import com.dremio.service.autocomplete.statements.grammar.AlterStatement;
import com.dremio.service.autocomplete.statements.grammar.DeleteStatement;
import com.dremio.service.autocomplete.statements.grammar.DropStatement;
import com.dremio.service.autocomplete.statements.grammar.ExternalReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.RawReflectionCreateStatement;
import com.dremio.service.autocomplete.statements.grammar.SelectQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.SetQueryStatement;
import com.dremio.service.autocomplete.statements.grammar.StatementList;
import com.dremio.service.autocomplete.statements.grammar.UnknownStatement;
import com.dremio.service.autocomplete.statements.grammar.UpdateStatement;

public final class StatementCompletionProvider implements StatementInputOutputVisitor<AutocompleteEngineContext, Completions> {
  public static final StatementCompletionProvider INSTANCE = new StatementCompletionProvider();

  private StatementCompletionProvider() {}

  @Override
  public Completions visit(StatementList statementList, AutocompleteEngineContext input) {
    throw new RuntimeException("We don't expect to visit a statement list.");
  }

  @Override
  public Completions visit(UnknownStatement unknownStatement, AutocompleteEngineContext input) {
    return Completions.EMPTY;
  }

  @Override
  public Completions visit(SelectQueryStatement selectQueryStatement, AutocompleteEngineContext input) {
    return SelectQueryStatementCompletionProvider.getCompletionsForIdentifier(
      selectQueryStatement,
      input);
  }

  @Override
  public Completions visit(SetQueryStatement setQueryStatement, AutocompleteEngineContext input) {
    throw new RuntimeException("We don't expect to visit a SetQueryStatement.");
  }

  @Override
  public Completions visit(DropStatement dropStatement, AutocompleteEngineContext input) {
    return DropStatementCompletionProvider.getCompletionsForIdentifier(
      dropStatement,
      input);
  }

  @Override
  public Completions visit(DeleteStatement deleteStatement, AutocompleteEngineContext input) {
    return DeleteStatementCompletionProvider.getCompletionsForIdentifier(
      deleteStatement,
      input);
  }

  @Override
  public Completions visit(UpdateStatement updateStatement, AutocompleteEngineContext input) {
    return UpdateStatementCompletionProvider.getCompletionsForIdentifier(
      updateStatement,
      input);
  }

  @Override
  public Completions visit(RawReflectionCreateStatement rawReflectionCreateStatement, AutocompleteEngineContext input) {
    return RawReflectionCreateCompletionProvider.getCompletionsForIdentifier(
      rawReflectionCreateStatement,
      input);
  }

  @Override
  public Completions visit(AlterStatement alterStatement, AutocompleteEngineContext input) {
    return AlterStatementCompletionProvider.getCompletionsForIdentifier(
      alterStatement,
      input);
  }

  @Override
  public Completions visit(AggregateReflectionCreateStatement aggregateReflectionCreateStatement, AutocompleteEngineContext input) {
    return AggregateReflectionCreateCompletionProvider.getCompletionsForIdentifier(
      aggregateReflectionCreateStatement,
      input);
  }

  @Override
  public Completions visit(ExternalReflectionCreateStatement externalReflectionCreateStatement, AutocompleteEngineContext input) {
    return ExternalReflectionCreateCompletionProvider.getCompletionsForIdentifier(
      externalReflectionCreateStatement,
      input);
  }
}
