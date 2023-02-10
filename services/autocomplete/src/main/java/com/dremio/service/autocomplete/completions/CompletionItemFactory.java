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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.service.autocomplete.catalog.Node;
import com.dremio.service.autocomplete.columns.ColumnAndTableAlias;
import com.dremio.service.autocomplete.nessie.Branch;
import com.dremio.service.autocomplete.nessie.Commit;
import com.dremio.service.autocomplete.nessie.NessieElement;
import com.dremio.service.autocomplete.nessie.NessieElementVisitor;
import com.dremio.service.autocomplete.nessie.Tag;
import com.dremio.service.autocomplete.tokens.DremioToken;
import com.dremio.service.autocomplete.tokens.TokenEscaper;
import com.dremio.service.functions.model.Function;
import com.dremio.service.functions.model.FunctionSignature;
import com.dremio.service.functions.model.Parameter;
import com.dremio.service.functions.snippets.Choice;
import com.dremio.service.functions.snippets.Placeholder;
import com.dremio.service.functions.snippets.Snippet;
import com.dremio.service.functions.snippets.SnippetElement;
import com.dremio.service.functions.snippets.Tabstop;
import com.dremio.service.functions.snippets.Text;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Factory for building CompletionItems
 */
public final class CompletionItemFactory {
  private static final Map<String, ImmutableList<CompletionItem>> FUNCTION_COMPLETION_ITEM_CACHE = new ConcurrentHashMap<>();
  private static final Map<Integer, CompletionItem> KEYWORD_COMPLETION_ITEM_CACHE = new ConcurrentHashMap<>();

  private CompletionItemFactory() {
  }

  public static CompletionItem createFromCatalogNode(Node catalogNode) {
    Preconditions.checkNotNull(catalogNode);
    // This CompletionItem is seeded on user data so it can't be cached
    // If we can add a per user cache, then we can speed this up.

    return ImmutableCompletionItem.builder()
      .label(catalogNode.getName())
      .kind(CompletionItemKind.CatalogEntry)
      .insertText(quoteIdentifier(catalogNode.getName()))
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

    String detail = String.format(
      "column (%s) in %s",
      columnAndTableAlias.getColumn().getType(),
      columnAndTableAlias.getTableAlias());

    return ImmutableCompletionItem.builder()
      .label(columnAndTableAlias.getColumn().getName())
      .kind(CompletionItemKind.Column)
      .insertText(insertText)
      .detail(detail)
      .data(columnAndTableAlias)
      .build();
  }

  public static List<CompletionItem> createFromFunction(Function function) {
    Preconditions.checkNotNull(function);
    String lookupKey = function.getName().toUpperCase();
    return FUNCTION_COMPLETION_ITEM_CACHE
      .computeIfAbsent(lookupKey, unusedVariable -> createFromFunctionImplementation(function));
  }

  private static ImmutableList<CompletionItem> createFromFunctionImplementation(Function function) {
    Preconditions.checkNotNull(function);

    Set<String> distinctLabels = new HashSet<>();
    return function
      .getSignatures()
      .stream()
      .map(signature -> {
        Snippet snippet = signature.getSnippetOverride();
        if (snippet == null) {
          snippet = createSnippet(function.getName(), signature);
        }

        StringBuilder labelBuilder = new StringBuilder();
        for (SnippetElement snippetElement : snippet.getSnippetElements()) {
          if (snippetElement instanceof Text) {
            labelBuilder.append(((Text) snippetElement).getValue());
          } else if (snippetElement instanceof Placeholder || snippetElement instanceof Tabstop) {
            labelBuilder.append("???");
          } else if (snippetElement instanceof Choice) {
            Choice choice = (Choice) snippetElement;
            labelBuilder
              .append("{")
              .append(String.join(",", choice.getChoices()))
              .append("}");
          } else {
            labelBuilder.append(snippetElement.toString());
          }
        }

        String detail = function.getDescription();
        if ((detail != null) && detail.startsWith("<") && detail.endsWith(">")) {
          // This means the doc writers have yet to fill out the yaml source.
          detail = null;
        }

        return ImmutableCompletionItem.builder()
          .label(labelBuilder.toString())
          .kind(CompletionItemKind.Function)
          .insertText(snippet.toString())
          .detail(detail)
          .build();
      })
      .filter(completionItem -> distinctLabels.add(completionItem.getLabel()))
      .collect(ImmutableList.toImmutableList());
  }

  public static CompletionItem createFromKeyword(int kind) {
    return KEYWORD_COMPLETION_ITEM_CACHE
      .computeIfAbsent(kind, CompletionItemFactory::createFromKeywordImplementation);
  }

  private static CompletionItem createFromKeywordImplementation(int kind) {
    DremioToken token = DremioToken.createFromParserKind(kind);
    return ImmutableCompletionItem.builder()
      .label(token.getImage())
      .kind(CompletionItemKind.Keyword)
      .build();
  }

  public static CompletionItem createFromNessieElement(NessieElement nessieElement) {
    Preconditions.checkNotNull(nessieElement);

    String label = nessieElement.accept(NessieElementToLabelVisitor.INSTANCE);

    return ImmutableCompletionItem.builder()
      .label(label)
      .kind(CompletionItemKind.NessieElement)
      .data(nessieElement)
      .build();
  }

  private static Snippet createSnippet(String name, FunctionSignature functionSignature) {
    Snippet.Builder snippetBuilder = Snippet.builder()
      .addText(name)
      .addText("(");

    for (int i = 0; i < functionSignature.getParameters().size(); i++) {
      if (i != 0) {
        snippetBuilder.addText(", ");
      }

      Parameter parameter = functionSignature.getParameters().get(i);
      snippetBuilder.addPlaceholder(parameter.getType().toString());
    }

    return snippetBuilder
      .addText(")")
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
