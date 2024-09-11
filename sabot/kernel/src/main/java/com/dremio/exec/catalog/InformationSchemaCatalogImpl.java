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

import static com.dremio.datastore.indexed.IndexKey.LOWER_CASE_SUFFIX;
import static com.dremio.exec.ExecConstants.INFO_SCHEMA_FIND_PAGE_SIZE;
import static com.dremio.exec.ExecConstants.VERSIONED_INFOSCHEMA_ENABLED;
import static com.dremio.exec.util.InformationSchemaCatalogUtil.getEscapeCharacter;

import com.dremio.common.exceptions.UserException;
import com.dremio.context.RequestContext;
import com.dremio.context.UserContext;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.ImmutableFindByCondition;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.TableType;
import com.dremio.service.catalog.View;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Implementation of {@link InformationSchemaCatalog} that relies on namespace. */
class InformationSchemaCatalogImpl implements InformationSchemaCatalog {
  private static final String DEFAULT_CATALOG_NAME = "DREMIO";
  private static final String CATALOG_DESCRIPTION = "The internal metadata used by Dremio";
  private static final String CATALOG_CONNECT = "";

  // Schemata are all entities in namespace that are not DATASET type.
  // TODO(DX-9909): cannot use a NOT filter
  private static final SearchTypes.SearchQuery SCHEMATA_FILTER =
      SearchQueryUtils.or(
          SearchQueryUtils.newTermQuery(
              NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.FOLDER.getNumber()),
          SearchQueryUtils.newTermQuery(
              NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.HOME.getNumber()),
          SearchQueryUtils.newTermQuery(
              NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.SOURCE.getNumber()),
          SearchQueryUtils.newTermQuery(
              NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.SPACE.getNumber()));

  private static final SearchTypes.SearchQuery DATASET_FILTER =
      SearchQueryUtils.newTermQuery(
          NamespaceIndexKeys.ENTITY_TYPE, NameSpaceContainer.Type.DATASET.getNumber());

  private static final Predicate<Document<NamespaceKey, NameSpaceContainer>> IS_NOT_INTERNAL =
      entry -> !entry.getKey().getRoot().startsWith("__");

  private static final ImmutableSet<String> SYSTEM_FIELDS =
      ImmutableSet.of(IncrementalUpdateUtils.UPDATE_COLUMN);

  private final NamespaceService userNamespace;
  private final NamespaceIdentity identity;
  private final PluginRetriever pluginRetriever;

  private final OptionManager optionManager;

  public InformationSchemaCatalogImpl(
      NamespaceService userNamespace,
      PluginRetriever pluginRetriever,
      OptionManager optionManager,
      NamespaceIdentity identity) {
    this.userNamespace = userNamespace;
    this.pluginRetriever = pluginRetriever;
    this.optionManager = optionManager;
    this.identity = identity;
  }

  private static LegacyFindByCondition getCondition(SearchQuery searchQuery) {
    return searchQuery == null
        ? null
        : new LegacyFindByCondition().setCondition(toSearchQuery(searchQuery));
  }

  private static SearchTypes.SearchQuery toSearchQuery(SearchQuery searchQuery) {
    switch (searchQuery.getQueryCase()) {
      case EQUALS:
        switch (searchQuery.getEquals().getValueCase()) {
          case INTVALUE:
            return SearchQueryUtils.newTermQuery(
                searchQuery.getEquals().getField(), searchQuery.getEquals().getIntValue());
          case STRINGVALUE:
            return SearchQueryUtils.newTermQuery(
                searchQuery.getEquals().getField(), searchQuery.getEquals().getStringValue());
          case VALUE_NOT_SET:
          default:
            throw new UnsupportedOperationException(
                String.format("%s is not supported", searchQuery.getEquals().getValueCase()));
        }
      case AND:
        return SearchQueryUtils.and(
            searchQuery.getAnd().getClausesList().stream()
                .map(InformationSchemaCatalogImpl::toSearchQuery)
                .collect(Collectors.toList()));
      case OR:
        return SearchQueryUtils.or(
            searchQuery.getOr().getClausesList().stream()
                .map(InformationSchemaCatalogImpl::toSearchQuery)
                .collect(Collectors.toList()));
      case LIKE:
        final String escape =
            searchQuery.getLike().getEscape().isEmpty() ? null : searchQuery.getLike().getEscape();
        return getLikeQuery(
            searchQuery.getLike().getField(),
            searchQuery.getLike().getPattern(),
            escape,
            searchQuery.getLike().getCaseInsensitive());
      default:
      case QUERY_NOT_SET:
        throw new UnsupportedOperationException(
            String.format("%s is not supported", searchQuery.getQueryCase()));
    }
  }

  /**
   * Converts a SQL like phrase into a similar Lucene WildcardQuery.
   *
   * @param fieldName The field to create the query on.
   * @param pattern the expected pattern
   * @param escape The Escape character to use.
   * @return The SearchQuery that matches the Like pattern.
   */
  public static SearchTypes.SearchQuery getLikeQuery(
      String fieldName, String pattern, String escape, boolean caseInsensitive) {
    StringBuilder sb = new StringBuilder();
    final char e = getEscapeCharacter(escape);
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
          // Percent is treated as wildchar which matches with (empty or any number) characters
        case '%':
          sb.append("*");
          break;

          // Underscore is treated as wildchar which matches with (only one) any character
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
      return SearchQueryUtils.newWildcardQuery(
          fieldName + LOWER_CASE_SUFFIX, sb.toString().toLowerCase());
    } else {
      return SearchQueryUtils.newWildcardQuery(fieldName, sb.toString());
    }
  }

  private Stream<VersionedPlugin> versionedPluginsRetriever() {
    if (pluginRetriever == null) {
      return null;
    }
    return pluginRetriever.getAllVersionedPlugins();
  }

  @Override
  public Iterator<Catalog> listCatalogs(SearchQuery searchQuery) {
    return Collections.singleton(
            Catalog.newBuilder()
                .setCatalogName(DEFAULT_CATALOG_NAME)
                .setCatalogDescription(CATALOG_DESCRIPTION)
                .setCatalogConnect(CATALOG_CONNECT)
                .build())
        .iterator();
  }

  @Override
  public Iterator<Schema> listSchemata(SearchQuery searchQuery) {
    final Iterator<Schema>[] res = new Iterator[] {Collections.emptyIterator()};
    Stream<VersionedPlugin> versionedPlugins = versionedPluginsRetriever();

    if (versionedPlugins != null && optionManager.getOption(VERSIONED_INFOSCHEMA_ENABLED)) {
      versionedPlugins
          .filter(versionedPlugin -> validateUserHasPrivilegeOn(versionedPlugin.getName()))
          .forEach(
              versionedPlugin -> {
                RequestContext.current()
                    .with(UserContext.CTX_KEY, new UserContext(identity.getId()))
                    .run(
                        () -> {
                          Stream<com.dremio.service.catalog.Schema> schemata =
                              versionedPlugin.getAllInformationSchemaSchemataInfo(searchQuery);
                          res[0] = Iterators.concat(res[0], schemata.iterator());
                        });
              });
    }

    final SearchTypes.SearchQuery query;
    if (searchQuery == null) {
      // search for schemata is faster than range scan, so push filter down
      query = SCHEMATA_FILTER;
    } else {
      query = SearchQueryUtils.and(toSearchQuery(searchQuery), SCHEMATA_FILTER);
    }

    final Iterable<Document<NamespaceKey, NameSpaceContainer>> searchResults =
        userNamespace.find(
            new ImmutableFindByCondition.Builder()
                .setCondition(query)
                .setPageSize((int) optionManager.getOption(INFO_SCHEMA_FIND_PAGE_SIZE))
                .build());

    final Set<String> alreadySent = new HashSet<>();
    return Iterators.concat(
        res[0],
        StreamSupport.stream(searchResults.spliterator(), false)
            .filter(IS_NOT_INTERNAL)
            .filter(entry -> !alreadySent.contains(entry.getKey().toUnescapedString()))
            .peek(entry -> alreadySent.add(entry.getKey().toUnescapedString()))
            .map(
                entry ->
                    Schema.newBuilder()
                        .setCatalogName(DEFAULT_CATALOG_NAME)
                        .setSchemaName(entry.getKey().toUnescapedString())
                        .setSchemaOwner("<owner>")
                        .setSchemaType(SchemaType.SIMPLE)
                        .setIsMutable(false)
                        .build())
            .iterator());
  }

  @Override
  public Iterator<Table> listTables(SearchQuery searchQuery) {

    final Iterator[] res = {Collections.emptyIterator()};
    Stream<VersionedPlugin> versionedPlugins = versionedPluginsRetriever();

    if (versionedPlugins != null && optionManager.getOption(VERSIONED_INFOSCHEMA_ENABLED)) {
      versionedPlugins
          .filter(versionedPlugin -> validateUserHasPrivilegeOn(versionedPlugin.getName()))
          .forEach(
              versionedPlugin -> {
                RequestContext.current()
                    .with(UserContext.CTX_KEY, new UserContext(identity.getId()))
                    .run(
                        () -> {
                          Stream<com.dremio.service.catalog.Table> tables =
                              versionedPlugin.getAllInformationSchemaTableInfo(searchQuery);
                          res[0] = Iterators.concat(res[0], tables.iterator());
                        });
              });
    }

    final Iterable<Document<NamespaceKey, NameSpaceContainer>> searchResults =
        userNamespace.find(
            new ImmutableFindByCondition.Builder()
                .setCondition(addDatasetFilter(searchQuery))
                .setPageSize((int) optionManager.getOption(INFO_SCHEMA_FIND_PAGE_SIZE))
                .build());

    return Iterators.concat(
        res[0],
        StreamSupport.stream(searchResults.spliterator(), false)
            .filter(IS_NOT_INTERNAL)
            .map(
                input -> {
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
            .iterator());
  }

  @Override
  public Iterator<View> listViews(SearchQuery searchQuery) {
    final Iterator[] res = {Collections.emptyIterator()};
    Stream<VersionedPlugin> versionedPlugins = versionedPluginsRetriever();

    if (versionedPlugins != null && optionManager.getOption(VERSIONED_INFOSCHEMA_ENABLED)) {
      versionedPlugins
          .filter(versionedPlugin -> validateUserHasPrivilegeOn(versionedPlugin.getName()))
          .forEach(
              versionedPlugin -> {
                RequestContext.current()
                    .with(UserContext.CTX_KEY, new UserContext(identity.getId()))
                    .run(
                        () -> {
                          Stream<com.dremio.service.catalog.View> views =
                              versionedPlugin.getAllInformationSchemaViewInfo(searchQuery);
                          res[0] = Iterators.concat(res[0], views.iterator());
                        });
              });
    }

    final Iterable<Document<NamespaceKey, NameSpaceContainer>> searchResults =
        userNamespace.find(
            new ImmutableFindByCondition.Builder()
                .setCondition(addDatasetFilter(searchQuery))
                .setPageSize((int) optionManager.getOption(INFO_SCHEMA_FIND_PAGE_SIZE))
                .build());

    return Iterators.concat(
        res[0],
        StreamSupport.stream(searchResults.spliterator(), false)
            .filter(IS_NOT_INTERNAL)
            .filter(entry -> entry.getValue().getDataset().getType() == DatasetType.VIRTUAL_DATASET)
            .map(
                entry ->
                    View.newBuilder()
                        .setCatalogName(DEFAULT_CATALOG_NAME)
                        .setSchemaName(entry.getKey().getParent().toUnescapedString())
                        .setTableName(entry.getKey().getName())
                        .setViewDefinition(
                            entry.getValue().getDataset().getVirtualDataset().getSql())
                        .build())
            .iterator());
  }

  @Override
  public Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery) {
    final Iterator[] res = {Collections.emptyIterator()};
    Stream<VersionedPlugin> versionedPlugins = versionedPluginsRetriever();

    if (versionedPlugins != null && optionManager.getOption(VERSIONED_INFOSCHEMA_ENABLED)) {
      versionedPlugins
          .filter(versionedPlugin -> validateUserHasPrivilegeOn(versionedPlugin.getName()))
          .forEach(
              versionedPlugin -> {
                RequestContext.current()
                    .with(UserContext.CTX_KEY, new UserContext(identity.getId()))
                    .run(
                        () -> {
                          Stream<com.dremio.service.catalog.TableSchema> columns =
                              versionedPlugin.getAllInformationSchemaColumnInfo(searchQuery);
                          res[0] = Iterators.concat(res[0], columns.iterator());
                        });
              });
    }

    final Iterable<Document<NamespaceKey, NameSpaceContainer>> searchResults =
        userNamespace.find(
            new ImmutableFindByCondition.Builder()
                .setCondition(addDatasetFilter(searchQuery))
                .setPageSize((int) optionManager.getOption(INFO_SCHEMA_FIND_PAGE_SIZE))
                .build());

    return Iterators.concat(
        res[0],
        StreamSupport.stream(searchResults.spliterator(), false)
            .filter(IS_NOT_INTERNAL)
            .filter(entry -> DatasetHelper.getSchemaBytes(entry.getValue().getDataset()) != null)
            .map(
                entry ->
                    TableSchema.newBuilder()
                        .setCatalogName(DEFAULT_CATALOG_NAME)
                        .setSchemaName(entry.getKey().getParent().toUnescapedString())
                        .setTableName(entry.getKey().getName())
                        .setBatchSchema(
                            rewriteBatchSchema(
                                DatasetHelper.getSchemaBytes(entry.getValue().getDataset())))
                        .build())
            .iterator());
  }

  private static ByteString rewriteBatchSchema(io.protostuff.ByteString byteString) {
    final BatchSchema batchSchema = BatchSchema.deserialize(byteString);
    final BatchSchema rewrittenSchema =
        new BatchSchema(
            batchSchema.getFields().stream()
                .filter(field -> !SYSTEM_FIELDS.contains(field.getName()))
                .collect(Collectors.toList()));

    return ByteString.copyFrom(rewrittenSchema.serialize());
  }

  /**
   * Adds a dataset filter to a given search query. If query is null, return just the dataset
   * filter.
   */
  private SearchTypes.SearchQuery addDatasetFilter(SearchQuery searchQuery) {
    if (searchQuery == null) {
      return DATASET_FILTER;
    } else {
      return SearchQueryUtils.and(toSearchQuery(searchQuery), DATASET_FILTER);
    }
  }

  private boolean validateUserHasPrivilegeOn(String sourceName) {
    try {
      userNamespace.getSource(new NamespaceKey(sourceName));
      return true;
    } catch (AccessControlException e) {
      return false;
    } catch (NamespaceException e) {
      throw UserException.validationError(e)
          .message("sourceName [%s] not found.", sourceName)
          .buildSilently();
    }
  }
}
