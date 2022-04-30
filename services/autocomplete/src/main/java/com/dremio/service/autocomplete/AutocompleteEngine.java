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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.dremio.service.autocomplete.catalog.CatalogNode;
import com.dremio.service.autocomplete.catalog.CatalogNodeResolver;
import com.dremio.service.autocomplete.catalog.CatalogNodeVisitor;
import com.dremio.service.autocomplete.catalog.DatasetCatalogNode;
import com.dremio.service.autocomplete.catalog.FileCatalogNode;
import com.dremio.service.autocomplete.catalog.FolderCatalogNode;
import com.dremio.service.autocomplete.catalog.HomeCatalogNode;
import com.dremio.service.autocomplete.catalog.SourceCatalogNode;
import com.dremio.service.autocomplete.catalog.SpaceCatalogNode;
import com.dremio.service.autocomplete.columns.Column;
import com.dremio.service.autocomplete.columns.ColumnResolver;
import com.dremio.service.autocomplete.functions.ParameterResolver;
import com.dremio.service.autocomplete.tokens.TokenResolver;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Completion engine for SQL queries.
 */
public final class AutocompleteEngine {
  private final TokenResolver tokenResolver;
  private final ColumnResolver columnResolver;
  private final CatalogNodeResolver catalogNodeResolver;
  private final ParameterResolver parameterResolver;

  public AutocompleteEngine(
    TokenResolver tokenResolver,
    ColumnResolver columnResolver,
    CatalogNodeResolver catalogNodeResolver,
    ParameterResolver parameterResolver) {
    Preconditions.checkNotNull(tokenResolver);
    Preconditions.checkNotNull(columnResolver);
    Preconditions.checkNotNull(catalogNodeResolver);
    Preconditions.checkNotNull(parameterResolver);

    this.tokenResolver = tokenResolver;
    this.columnResolver = columnResolver;
    this.catalogNodeResolver = catalogNodeResolver;
    this.parameterResolver = parameterResolver;
  }

  public List<CompletionItem> generateCompletions(String corpus, int cursorIndex) {
    Preconditions.checkNotNull(corpus);
    Preconditions.checkArgument(cursorIndex >= 0);

    Pair<ImmutableList<DremioToken>, Integer> tokensAndIndex = AutocompleteEngine.tokenizeAndGetCursorIndex(
      corpus,
      cursorIndex);

    Optional<QueryExtractor.TokenRange> fromClauseTokenRange = tryGetFromClauseTokenRange(
      tokensAndIndex.left,
      tokensAndIndex.right);

    ImmutableList<DremioToken> corpusUpUntilCursor = tokensAndIndex.left.subList(0, tokensAndIndex.right);
    boolean hasTrailingWhitespace = (corpus.length() != 0) && Character.isWhitespace(corpus.charAt(cursorIndex - 1));
    List<DremioToken> tokens = tokenResolver.resolve(corpusUpUntilCursor, hasTrailingWhitespace);

    List<CompletionItem> completionItems = new ArrayList<>();
    for (DremioToken token : tokens) {
      CompletionItemKind completionItemKind = getCompletionItemKind(
        token,
        fromClauseTokenRange,
        tokensAndIndex.right);
      switch (completionItemKind) {
      case CatalogEntry:
      {
        ImmutableList<DremioToken> fromClauseTokens = tokensAndIndex.left.subList(
          fromClauseTokenRange.get().getStartIndexInclusive(),
          fromClauseTokenRange.get().getEndIndexExclusive());
        List<CatalogNode> catalogNodes = catalogNodeResolver.resolve(fromClauseTokens);
        for (CatalogNode catalogNode : catalogNodes) {
          completionItems.add(buildCatalogEntryCompletionItem(catalogNode));
        }
      }
      break;

      case Column:
      {
        ImmutableList<DremioToken> fromClauseTokens = tokensAndIndex.left.subList(
          fromClauseTokenRange.get().getStartIndexInclusive(),
          fromClauseTokenRange.get().getEndIndexExclusive());
        Map<List<String>, Set<Column>> tableToColumns = columnResolver.resolve(fromClauseTokens);
        ImmutableSet.Builder<Column> allColumns = new ImmutableSet.Builder<>();
        for (Set<Column> columns : tableToColumns.values()) {
          allColumns.addAll(columns);
        }

        // We need to determine if we are inside a function,
        // so we only recommend the columns that are relevant.
        Optional<ParameterResolver.Resolutions> optionalResolutions = parameterResolver.resolve(
          corpusUpUntilCursor,
          allColumns.build(),
          fromClauseTokens);
        if (optionalResolutions.isPresent()) {
          ParameterResolver.Resolutions resolutions = optionalResolutions.get();

          resolutions
            .getColumns()
            .stream()
            .sorted(Comparator.comparing(Column::getName))
            .forEach(column -> {
              List<String> tablePath = null;
              for (List<String> path : tableToColumns.keySet()) {
                if (tableToColumns.get(path).contains(column)) {
                  tablePath = path;
                  break;
                }
              }

              completionItems.add(buildColumnCompletionItem(column, tablePath));
            });

          resolutions
            .getFunctions()
            .stream()
            .sorted(Comparator.comparing(SqlFunction::getName))
            .forEach(sqlFunction -> {
              completionItems.add(buildFunctionCompletionItem(sqlFunction));
            });
        } else {
          for (List<String> tablePath : tableToColumns.keySet()) {
            tableToColumns
              .get(tablePath)
              .stream()
              .sorted(Comparator.comparing(Column::getName))
              .forEach(column -> {
                completionItems.add(buildColumnCompletionItem(column, tablePath));
              });
          }
        }
      }
      break;

      case Function:
      {
        SqlFunction sqlFunction = parameterResolver
          .getSqlFunctionDictionary()
          .tryGetValue(token.getImage())
          .get();
        completionItems.add(buildFunctionCompletionItem(sqlFunction));
      }
      break;

      case Keyword:
      {
        completionItems.add(buildKeywordCompletionItem(token));
      }
      break;

      default:
        throw new UnsupportedOperationException("Unknown CompletionItemKind: " + completionItemKind);
      }
    }

    return completionItems;
  }

  private static Pair<ImmutableList<DremioToken>, Integer> tokenizeAndGetCursorIndex(
    final String corpus,
    final int cursorIndex) {
    Preconditions.checkNotNull(corpus);
    Preconditions.checkArgument(cursorIndex >= 0);

    final ImmutableList<DremioToken> tokens = ImmutableList.copyOf(SqlQueryTokenizer.tokenize(corpus));
    // Remove whitespace from cursorIndex count
    final int whitespaceCount = (int) corpus.substring(0, cursorIndex).chars().filter(Character::isWhitespace).count();
    final int cursorIndexWithoutWhitespace = cursorIndex - whitespaceCount;
    int tokenCursorIndex = 0;
    for (int charactersRead = 0; (tokenCursorIndex < tokens.size()) && (charactersRead < cursorIndexWithoutWhitespace); tokenCursorIndex++) {
      DremioToken token = tokens.get(tokenCursorIndex);
      charactersRead += token.getImage().length();
    }

    return new Pair<>(tokens, tokenCursorIndex);
  }

  private static Optional<QueryExtractor.TokenRange> tryGetFromClauseTokenRange(
    ImmutableList<DremioToken> tokens,
    Integer cursorIndex) {
    if (!hasFromClause(tokens)) {
      return Optional.empty();
    }

    QueryExtractor.TokenRanges tokenRanges = QueryExtractor.extractTokenRanges(
      tokens,
      cursorIndex);
    return Optional.of(tokenRanges.getFromClause());
  }

  private static boolean hasFromClause(ImmutableList<DremioToken> tokens) {
    return tokens.stream().anyMatch(token -> token.getKind() == ParserImplConstants.FROM);
  }

  private CompletionItemKind getCompletionItemKind(
    DremioToken token,
    Optional<QueryExtractor.TokenRange> fromClauseTokenRange,
    int cursorIndex) {
    if (token.getKind() != ParserImplConstants.IDENTIFIER) {
      if (parameterResolver.getSqlFunctionDictionary().tryGetValue(token.getImage()).isPresent()) {
        return CompletionItemKind.Function;
      }

      return CompletionItemKind.Keyword;
    }

    if (!fromClauseTokenRange.isPresent()) {
      return CompletionItemKind.Keyword;
    }

    boolean insideFromClause = cursorIndex > fromClauseTokenRange.get().getStartIndexInclusive()
      && cursorIndex <= fromClauseTokenRange.get().getEndIndexExclusive();

    return insideFromClause ? CompletionItemKind.CatalogEntry : CompletionItemKind.Column;
  }

  private static final class CatalogEntryCompletionItemData {
    private final String name;
    private final String type;

    @JsonCreator
    private CatalogEntryCompletionItemData(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }

  private static final class ColumnCompletionItemData {
    private final String name;
    private final String type;
    private final List<String> tablePath;

    @JsonCreator
    private ColumnCompletionItemData(
      @JsonProperty("name") String name,
      @JsonProperty("type") String type,
      @JsonProperty("tablePath") List<String> tablePath) {
      this.name = name;
      this.type = type;
      this.tablePath = tablePath;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public List<String> getTablePath() {
      return tablePath;
    }
  }

  private static final class KeywordCompletionItemData {
    private final int kind;

    @JsonCreator
    private KeywordCompletionItemData(
      @JsonProperty("kind") Integer kind) {
      this.kind = kind;
    }

    public int getKinds() {
      return kind;
    }
  }

  private static final class CatalogNodeTypeStringVisitor implements CatalogNodeVisitor<String> {
    public static final CatalogNodeTypeStringVisitor INSTANCE = new CatalogNodeTypeStringVisitor();
    private CatalogNodeTypeStringVisitor(){}

    @Override
    public String visit(DatasetCatalogNode datasetCatalogNode) {
      return datasetCatalogNode.getType().toString() + " Dataset";
    }

    @Override
    public String visit(FileCatalogNode fileCatalogNode) {
      return "File";
    }

    @Override
    public String visit(FolderCatalogNode folderCatalogNode) {
      return "Folder";
    }

    @Override
    public String visit(HomeCatalogNode homeCatalogNode) {
      return "Home";
    }

    @Override
    public String visit(SourceCatalogNode sourceCatalogNode) {
      return "Source";
    }

    @Override
    public String visit(SpaceCatalogNode spaceCatalogNode) {
      return "Space";
    }
  }

  private static CompletionItem buildCatalogEntryCompletionItem(CatalogNode catalogNode) {
    return ImmutableCompletionItem.builder()
      .label(catalogNode.getName())
      .kind(CompletionItemKind.CatalogEntry)
      .detail("Some CatalogEntry")
      .data(new CatalogEntryCompletionItemData(
        catalogNode.getName(),
        catalogNode.accept(CatalogNodeTypeStringVisitor.INSTANCE)))
      .build();
  }

  private static CompletionItem buildColumnCompletionItem(Column column, List<String> tablePath) {
    return ImmutableCompletionItem.builder()
      .label(column.getName())
      .kind(CompletionItemKind.Column)
      .detail("Some Column")
      .data(new ColumnCompletionItemData(
        column.getName(),
        column.getType().getSqlTypeName().getName(),
        tablePath))
      .build();
  }

  private static CompletionItem buildFunctionCompletionItem(SqlFunction sqlFunction) {
    return ImmutableCompletionItem.builder()
      .label(sqlFunction.getName())
      .kind(CompletionItemKind.Function)
      .detail(sqlFunction.getName())
      .data(sqlFunction.getName())
      .build();
  }

  private static CompletionItem buildKeywordCompletionItem(DremioToken token) {
    return ImmutableCompletionItem.builder()
      .label(token.getImage())
      .kind(CompletionItemKind.Keyword)
      .detail("Some SQL keyword")
      .data(new KeywordCompletionItemData(token.getKind()))
      .build();
  }
}
