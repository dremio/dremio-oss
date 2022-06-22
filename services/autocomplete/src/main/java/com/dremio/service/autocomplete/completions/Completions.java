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

package com.dremio.service.autocomplete.completions;

import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.util.Preconditions;

import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.columns.ColumnAndTableAliasComparator;
import com.dremio.service.autocomplete.functions.Function;
import com.dremio.service.autocomplete.functions.FunctionContext;
import com.dremio.service.autocomplete.functions.TokenTypeDetector;
import com.dremio.service.autocomplete.nessie.NessieElement;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.google.common.collect.ImmutableList;

/**
 * Completions for Autocomplete.
 */
public final class Completions {
    public static final Completions EMPTY = new Completions(ImmutableList.of(), null);

    private final ImmutableList<CompletionItem> completionItems;
    private final FunctionContext functionContext;

    public Completions(
            ImmutableList<CompletionItem> completionItems,
            FunctionContext functionContext) {
        Preconditions.checkNotNull(completionItems);

        this.completionItems = completionItems;
        this.functionContext = functionContext;
    }

    public ImmutableList<CompletionItem> getCompletionItems() {
        return completionItems;
    }

    public FunctionContext getFunctionContext() {
        return functionContext;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private List<CompletionItem> completionItems;
        private FunctionContext functionContext;
        private String prefixFilter;


        private Builder() {
            this.completionItems = new ArrayList<>();
            this.functionContext = null;
            this.prefixFilter = null;
        }

        public Builder addCatalogNodes(List<Node> catalogNodes) {
            Preconditions.checkNotNull(catalogNodes);

            completionItems.addAll(catalogNodes
                    .stream()
                    .sorted(Comparator.comparing(Node::getType).thenComparing(Node::getName))
                    .map(CompletionItemFactory::createFromCatalogNode)
                    .collect(Collectors.toList()));

            return this;
        }

        public Builder addColumnAndTableAliases(ImmutableList<ColumnAndTableAlias> columnAndTableAliases, String scope) {
            Preconditions.checkNotNull(columnAndTableAliases);

            return this.addItems(
                    columnAndTableAliases
                            .stream()
                            .filter(entry -> scope == null || scope.equalsIgnoreCase(entry.getTableAlias()))
                            .sorted(ColumnAndTableAliasComparator.INSTANCE)
                            .map(c -> CompletionItemFactory.createFromColumnAndPath(c, scope != null))
                            .collect(ImmutableList.toImmutableList()));
        }

        public Builder addFunctions(ImmutableList<Function> functions, ImmutableList<DremioToken> contextTokens) {
            Preconditions.checkNotNull(functions);
            Preconditions.checkNotNull(contextTokens);

            return this.addItems(
                    functions.stream()
                            .filter(function -> !TokenTypeDetector.isKeyword(function.getName(), contextTokens).orElse(false))
                            .sorted(Comparator.comparing(Function::getName))
                            .map(CompletionItemFactory::createFromFunction)
                            .collect(ImmutableList.toImmutableList()));
        }

        public Builder addNessieElements(ImmutableList<NessieElement> nessieElements) {
            Preconditions.checkNotNull(nessieElements);

            return this.addItems(
                    nessieElements
                            .stream()
                            .map(CompletionItemFactory::createFromNessieElement)
                            .collect(ImmutableList.toImmutableList()));
        }

        public Builder addKeywords(ImmutableList<DremioToken> keywords) {
            Preconditions.checkNotNull(keywords);

            return this.addItems(
                    keywords
                            .stream()
                            .map(CompletionItemFactory::createFromDremioToken)
                            .collect(ImmutableList.toImmutableList()));
        }

        public Builder merge(Completions completions) {
            Preconditions.checkNotNull(completions);

            return this
                    .addItems(completions.getCompletionItems())
                    .addFunctionContext(completions.getFunctionContext());
        }

        public Builder addItems(ImmutableList<CompletionItem> completionItems) {
            Preconditions.checkNotNull(completionItems);

            this.completionItems.addAll(completionItems);
            return this;
        }

        public Builder addFunctionContext(FunctionContext functionContext) {
            if (functionContext != null) {
                this.functionContext = functionContext;
            }

            return this;
        }

        public Builder addPrefixFilter(String prefixFilter) {
            this.prefixFilter = Preconditions.checkNotNull(prefixFilter);
            return this;
        }

        public Completions build() {
          Stream<CompletionItem> items = completionItems.stream();
          if (prefixFilter != null) {
            items = items
              .filter(completionItem -> startsWithIgnoreCase(completionItem.getLabel(), prefixFilter));
          }

          return new Completions(
            items.collect(ImmutableList.toImmutableList()),
            functionContext);
        }
    }
}
