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
package com.dremio.service.autocomplete;

import static com.dremio.service.autocomplete.catalog.mock.MockMetadataCatalog.createCatalog;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;

import com.dremio.service.autocomplete.catalog.AutocompleteSchemaProvider;
import com.dremio.service.autocomplete.catalog.mock.MockAutocompleteSchemaProvider;
import com.dremio.service.autocomplete.catalog.mock.MockMetadataCatalog;
import com.dremio.service.autocomplete.completions.CompletionItem;
import com.dremio.service.autocomplete.completions.Completions;
import com.dremio.service.autocomplete.functions.FunctionContext;
import com.dremio.service.autocomplete.functions.ParameterResolverTests;
import com.dremio.service.autocomplete.nessie.MockNessieElementReader;
import com.dremio.test.GoldenFileTestBuilder.MultiLineString;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

/**
 * Tests for the autocomplete engine.
 */
public abstract class AutocompleteEngineTests {
  protected CompletionsForBaselines executeTestWithRootContext(MultiLineString query) {
    return executeTest(query, ImmutableList.of());
  }

  protected CompletionsForBaselines executeTestWithFolderContext(MultiLineString query) {
    return executeTest(query, ImmutableList.of("space", "folder"));
  }

  protected CompletionsForBaselines executeTest(MultiLineString query, ImmutableList<String> context) {
    StringAndPos stringAndPos = SqlParserUtil.findPos(query.toString());
    Completions completions = generateCompletions(
      context,
      stringAndPos.sql,
      stringAndPos.cursor);
    return CompletionsForBaselines.create(completions);
  }

  protected Completions generateCompletions(List<String> context, String sql, int cursorPosition) {
    MockMetadataCatalog.CatalogData data = createCatalog(context);
    AutocompleteSchemaProvider schemaProvider = new MockAutocompleteSchemaProvider(
      data.getContext(),
      data.getHead());
    SqlOperatorTable operatorTable = OperatorTableFactory.createWithProductionFunctions(ParameterResolverTests.FUNCTIONS);
    MockMetadataCatalog catalog = new MockMetadataCatalog(data);
    return AutocompleteEngine.generateCompletions(
      new AutocompleteEngineContext(
        schemaProvider,
        Suppliers.ofInstance(MockNessieElementReader.INSTANCE),
        operatorTable,
        catalog),
      sql,
      cursorPosition);
  }

  /**
   * The output for an autocompletion test.
   */
  public static final class CompletionsForBaselines {
    private final List<CompletionItem> completions;
    private final FunctionContext functionContext;
    private final boolean hasMoreResults;

    private CompletionsForBaselines(
      List<CompletionItem> completions,
      FunctionContext functionContext,
      boolean hasMoreResults) {
      this.completions = completions;
      this.functionContext = functionContext;
      this.hasMoreResults = hasMoreResults;
    }

    public List<CompletionItem> getCompletions() {
      return completions;
    }

    public FunctionContext getFunctionContext() {
      return functionContext;
    }

    public boolean isHasMoreResults() {
      return hasMoreResults;
    }

    public static CompletionsForBaselines create(Completions completions) {
      final int numCompletions = 5;
      return new CompletionsForBaselines(
        completions
          .getCompletionItems()
          .stream()
          .limit(numCompletions)
          .collect(Collectors.toList()),
        completions.getFunctionContext(),
        completions.getCompletionItems().size() - numCompletions > 0);
    }
  }
}
