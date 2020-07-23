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

package com.dremio.exec.catalog;

import static com.dremio.service.namespace.DatasetIndexKeys.LOWER_CASE_SUFFIX;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.TableType;
import com.dremio.service.catalog.View;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

/**
 * Implementation of {@link InformationSchemaCatalog} that relies on namespace.
 */
class InformationSchemaCatalogImpl implements InformationSchemaCatalog {

  private static final String DEFAULT_CATALOG_NAME = "DREMIO";
  private static final String CATALOG_DESCRIPTION = "The internal metadata used by Dremio";
  private static final String CATALOG_CONNECT = "";

  // Schemata are all entities in namespace that are not DATASET type.
  // TODO(DX-9909): cannot use a NOT filter
  private static final SearchTypes.SearchQuery SCHEMATA_FILTER = SearchQueryUtils.or(
    SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.FOLDER.getNumber()),
    SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.HOME.getNumber()),
    SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.SOURCE.getNumber()),
    SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.SPACE.getNumber())
  );

  private static final Predicate<Map.Entry<NamespaceKey, NameSpaceContainer>> IS_NOT_INTERNAL =
    entry -> !entry.getKey().getRoot().startsWith("__");
  private static final Predicate<Map.Entry<NamespaceKey, NameSpaceContainer>> IS_DATASET =
    entry -> entry.getValue().getType() == NameSpaceContainer.Type.DATASET;

  private static final ImmutableSet<String> SYSTEM_FIELDS =
    ImmutableSet.of(IncrementalUpdateUtils.UPDATE_COLUMN);

  private final NamespaceService userNamespace;

  InformationSchemaCatalogImpl(NamespaceService userNamespace) {
    this.userNamespace = userNamespace;
  }

  private static LegacyFindByCondition getCondition(SearchQuery searchQuery) {
    return searchQuery == null ? null : new LegacyFindByCondition().setCondition(toSearchQuery(searchQuery));
  }

  private static SearchTypes.SearchQuery toSearchQuery(SearchQuery searchQuery) {
    switch (searchQuery.getQueryCase()) {
    case EQUALS:
      switch (searchQuery.getEquals().getValueCase()) {
      case INTVALUE:
        return SearchQueryUtils.newTermQuery(searchQuery.getEquals().getField(),
          searchQuery.getEquals().getIntValue());
      case STRINGVALUE:
        return SearchQueryUtils.newTermQuery(searchQuery.getEquals().getField(),
          searchQuery.getEquals().getStringValue());
      case VALUE_NOT_SET:
      default:
        throw new UnsupportedOperationException(String.format("%s is not supported",
          searchQuery.getEquals().getValueCase()));
      }
    case AND:
      return SearchQueryUtils.and(searchQuery.getAnd()
        .getClausesList()
        .stream()
        .map(InformationSchemaCatalogImpl::toSearchQuery)
        .collect(Collectors.toList()));
    case OR:
      return SearchQueryUtils.or(searchQuery.getOr()
        .getClausesList()
        .stream()
        .map(InformationSchemaCatalogImpl::toSearchQuery)
        .collect(Collectors.toList()));
    case LIKE:
      final String escape = searchQuery.getLike().getEscape().isEmpty() ? null :
        searchQuery.getLike().getEscape();
      return getLikeQuery(searchQuery.getLike().getField(),
        searchQuery.getLike().getPattern(), escape, searchQuery.getLike().getCaseInsensitive());
    default:
    case QUERY_NOT_SET:
      throw new UnsupportedOperationException(String.format("%s is not supported",
        searchQuery.getQueryCase()));
    }
  }

  /**
   * Converts a SQL like phrase into a similar Lucene WildcardQuery.
   *
   * @param fieldName The field to create the query on.
   * @param pattern   the expected pattern
   * @param escape    The Escape character to use.
   * @return The SearchQuery that matches the Like pattern.
   */
  public static SearchTypes.SearchQuery getLikeQuery(
    String fieldName,
    String pattern,
    String escape,
    boolean caseInsensitive
  ) {
    Preconditions.checkArgument(escape == null || escape.length() == 1, "An escape must be a single character.");
    StringBuilder sb = new StringBuilder();
    final char e = escape == null ? '\\' : escape.charAt(0);
    boolean escaped = false;

    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);

      if (escaped) {
        sb.append(c);
        escaped = false;
        continue;
      }

      if (c == e) {
        sb.append('\\');
        escaped = true;
        continue;
      }

      switch (c) {
        case '%':
          sb.append("*");
          break;

        case '_':
          sb.append("?");
          break;

        // ESCAPE * if it occurs
        case '*':
          sb.append("\\*");
          break;

        // ESCAPE ? if it occurs
        case '?':
          sb.append("\\?");
          break;

        default:
          sb.append(c);
          break;
      }
    }

    if (caseInsensitive) {
      return SearchQueryUtils.newWildcardQuery(fieldName + LOWER_CASE_SUFFIX, sb.toString().toLowerCase());
    } else {
      return SearchQueryUtils.newWildcardQuery(fieldName, sb.toString());
    }
  }

  @Override
  public Iterator<Catalog> listCatalogs(SearchQuery searchQuery) {
    return Collections.singleton(Catalog.newBuilder()
      .setCatalogName(DEFAULT_CATALOG_NAME)
      .setCatalogDescription(CATALOG_DESCRIPTION)
      .setCatalogConnect(CATALOG_CONNECT)
      .build()
    ).iterator();
  }

  @Override
  public Iterator<Schema> listSchemata(SearchQuery searchQuery) {
    final SearchTypes.SearchQuery query;
    if (searchQuery == null) {
      // search for schemata is faster than range scan, so push filter down
      query = SCHEMATA_FILTER;
    } else {
      query = SearchQueryUtils.and(toSearchQuery(searchQuery), SCHEMATA_FILTER);
    }

    final Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> searchResults =
      userNamespace.find(new LegacyFindByCondition().setCondition(query));

    final Set<String> alreadySent = new HashSet<>();
    return StreamSupport.stream(searchResults.spliterator(), false)
      .filter(IS_NOT_INTERNAL)
      .filter(entry -> !alreadySent.contains(entry.getKey().toUnescapedString()))
      .peek(entry -> alreadySent.add(entry.getKey().toUnescapedString()))
      .map(entry -> Schema.newBuilder()
        .setCatalogName(DEFAULT_CATALOG_NAME)
        .setSchemaName(entry.getKey().toUnescapedString())
        .setSchemaOwner("<owner>")
        .setSchemaType(SchemaType.SIMPLE)
        .setIsMutable(false)
        .build())
      .iterator();
  }

  @Override
  public Iterator<Table> listTables(SearchQuery searchQuery) {
    final Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> searchResults =
      userNamespace.find(getCondition(searchQuery));

    return StreamSupport.stream(searchResults.spliterator(), false)
      .filter(IS_NOT_INTERNAL)
      .filter(IS_DATASET)
      .map(input -> {
        final String sourceName = input.getKey().getRoot();

        final TableType tableType;
        if (input.getValue().getDataset().getType() == DatasetType.VIRTUAL_DATASET) {
          tableType = TableType.VIEW;
        } else if ("sys".equals(sourceName) || "INFORMATION_SCHEMA".equals(sourceName)) {
          tableType = TableType.SYSTEM_TABLE;
        } else {
          tableType = TableType.TABLE;
        }

        return Table.newBuilder()
          .setCatalogName(DEFAULT_CATALOG_NAME)
          .setSchemaName(input.getKey().getParent().toUnescapedString())
          .setTableName(input.getKey().getName())
          .setTableType(tableType)
          .build();
      })
      .iterator();
  }

  @Override
  public Iterator<View> listViews(SearchQuery searchQuery) {
    final Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> searchResults =
      userNamespace.find(getCondition(searchQuery));

    return StreamSupport.stream(searchResults.spliterator(), false)
      .filter(IS_NOT_INTERNAL)
      .filter(IS_DATASET)
      .filter(entry -> entry.getValue().getDataset().getType() == DatasetType.VIRTUAL_DATASET)
      .map(entry -> View.newBuilder()
        .setCatalogName(DEFAULT_CATALOG_NAME)
        .setSchemaName(entry.getKey().getParent().toUnescapedString())
        .setTableName(entry.getKey().getName())
        .setViewDefinition(entry.getValue().getDataset().getVirtualDataset().getSql())
        .build())
      .iterator();
  }

  @Override
  public Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery) {
    final Iterable<Map.Entry<NamespaceKey, NameSpaceContainer>> searchResults =
      userNamespace.find(getCondition(searchQuery));

    return StreamSupport.stream(searchResults.spliterator(), false)
      .filter(IS_NOT_INTERNAL)
      .filter(IS_DATASET)
      .filter(entry -> DatasetHelper.getSchemaBytes(entry.getValue().getDataset()) != null)
      .map(entry -> TableSchema.newBuilder()
        .setCatalogName(DEFAULT_CATALOG_NAME)
        .setSchemaName(entry.getKey().getParent().toUnescapedString())
        .setTableName(entry.getKey().getName())
        .setBatchSchema(rewriteBatchSchema(DatasetHelper.getSchemaBytes(entry.getValue().getDataset())))
        .build())
      .iterator();
  }

  private static ByteString rewriteBatchSchema(io.protostuff.ByteString byteString) {
    final BatchSchema batchSchema = BatchSchema.deserialize(byteString);
    final BatchSchema rewrittenSchema = new BatchSchema(batchSchema.getFields()
      .stream()
      .filter(field -> !SYSTEM_FIELDS.contains(field.getName()))
      .collect(Collectors.toList())
    );

    return ByteString.copyFrom(rewrittenSchema.serialize());
  }
}
