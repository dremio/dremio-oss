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

import static com.dremio.common.utils.SqlUtils.quoteIdentifier;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.functions.Function;
import com.dremio.service.autocomplete.nessie.Branch;
import com.dremio.service.autocomplete.nessie.Commit;
import com.dremio.service.autocomplete.nessie.NessieElement;
import com.dremio.service.autocomplete.nessie.NessieElementVisitor;
import com.dremio.service.autocomplete.nessie.Tag;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenEscaper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Factory for building CompletionItems
 */
public final class CompletionItemFactory {
  private static final Map<String, CompletionItem> FUNCTION_COMPLETION_ITEM_CACHE = new ConcurrentHashMap<>();
  private static final Map<Integer, CompletionItem> KEYWORD_COMPLETION_ITEM_CACHE = new ConcurrentHashMap<>();

  private CompletionItemFactory() {
  }

  public static CompletionItem createFromCatalogNode(Node catalogNode) {
    Preconditions.checkNotNull(catalogNode);
    // This CompletionItem is seeded on user data so it can't be cached
    // If we can add a per user cache, then we can speed this up.

    return ImmutableCompletionItem.builder()
      .label(catalogNode.getName())
      .insertText(quoteIdentifier(catalogNode.getName()))
      .kind(CompletionItemKind.CatalogEntry)
      .detail("Some CatalogEntry")
      .data(new CatalogEntryCompletionItemData(
        catalogNode.getName(),
        catalogNode.getType()))
      .build();
  }

  public static CompletionItem createFromColumnAndPath(ColumnAndTableAlias columnAndTableAlias, boolean ignoreTableAlias) {
    Preconditions.checkNotNull(columnAndTableAlias);
    // This CompletionItem is seeded on user data so it can't be cached
    // If we can add a per user cache, then we can speed this up.

    String insertText = ignoreTableAlias
      ? quoteIdentifier(columnAndTableAlias.getColumn().getName())
      : TokenEscaper.escape(ImmutableList.of(columnAndTableAlias.getTableAlias(), columnAndTableAlias.getColumn().getName()));

    return ImmutableCompletionItem.builder()
      .label(columnAndTableAlias.getColumn().getName())
      .insertText(insertText)
      .kind(CompletionItemKind.Column)
      .detail("Some Column")
      .data(columnAndTableAlias)
      .build();
  }

  public static CompletionItem createFromFunction(Function function) {
    Preconditions.checkNotNull(function);
    String lookupKey = function.getName().toUpperCase();
    return FUNCTION_COMPLETION_ITEM_CACHE
      .computeIfAbsent(lookupKey, unusedVariable -> createFromFunctionImplementation(function));
  }

  private static CompletionItem createFromFunctionImplementation(Function function) {
    Preconditions.checkNotNull(function);
    Optional<String> optionalSyntax = Optional.ofNullable(function.getSyntax());
    StringBuilder insertTextBuilder = new StringBuilder()
      .append(function.getName())
      .append("(");

    optionalSyntax.ifPresent(insertTextBuilder::append);

    insertTextBuilder.append(")");

    return ImmutableCompletionItem.builder()
      .label(function.getName())
      .insertText(insertTextBuilder.toString())
      .kind(CompletionItemKind.Function)
      .detail(function.getName())
      .data(function.getName())
      .build();
  }

  public static CompletionItem createFromDremioToken(DremioToken token) {
    Preconditions.checkNotNull(token);
    if (token.getKind() == ParserImplConstants.IDENTIFIER) {
      // We can't cache an identifier
      return createFromDremioTokenImplementation(token);
    }

    return KEYWORD_COMPLETION_ITEM_CACHE
      .computeIfAbsent(token.getKind(), value -> createFromDremioTokenImplementation(token));
  }

  private static CompletionItem createFromDremioTokenImplementation(DremioToken token) {
    Preconditions.checkNotNull(token);

    return ImmutableCompletionItem.builder()
      .label(token.getImage())
      .insertText(token.getImage())
      .kind(CompletionItemKind.Keyword)
      .detail("Some SQL keyword")
      .data(token.getImage())
      .build();
  }

  public static CompletionItem createFromNessieElement(NessieElement nessieElement) {
    Preconditions.checkNotNull(nessieElement);

    String label = nessieElement.accept(NessieElementToLabelVisitor.INSTANCE);

    return ImmutableCompletionItem.builder()
      .label(label)
      .insertText(label)
      .kind(CompletionItemKind.NessieElement)
      .detail("A Nessie Element")
      .data(nessieElement)
      .build();
  }

  private static final class NessieElementToLabelVisitor implements NessieElementVisitor<String> {
    public static final NessieElementToLabelVisitor INSTANCE = new NessieElementToLabelVisitor();

    private NessieElementToLabelVisitor() {
    }

    @Override
    public String visit(Branch branch) {
      return branch.getName();
    }

    @Override
    public String visit(Commit commit) {
      return commit.getHash().getValue();
    }

    @Override
    public String visit(Tag tag) {
      return tag.getName();
    }
  }
}
