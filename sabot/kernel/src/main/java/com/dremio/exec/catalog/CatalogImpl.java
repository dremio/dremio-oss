/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.calcite.schema.Function;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePlugin.UpdateStatus;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.ischema.tables.InfoSchemaTable;
import com.dremio.exec.store.ischema.tables.SchemataTable.Schema;
import com.dremio.exec.store.ischema.tables.TablesTable.Table;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Default, non caching, implementation of {@link Catalog}
 */
@ThreadSafe
public class CatalogImpl implements Catalog {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogImpl.class);

  private final SabotContext context;
  private final MetadataRequestOptions options;
  private final PluginRetriever pluginRetriever;
  private final String username;

  private final NamespaceService systemNamespaceService;
  private final NamespaceService userNamespaceService;
  private final DatasetManager datasets;
  private final CatalogServiceImpl.SourceModifier sourceModifier;

  CatalogImpl(
      SabotContext context,
      MetadataRequestOptions options,
      PluginRetriever pluginRetriever,
      CatalogServiceImpl.SourceModifier sourceModifier
      ) {
    this(context, options, pluginRetriever, options.getSchemaConfig().getUserName(), sourceModifier);
  }

  private CatalogImpl(
    SabotContext context,
    MetadataRequestOptions options,
    PluginRetriever pluginRetriever,
    String username,
    CatalogServiceImpl.SourceModifier sourceModifier
  ) {
    this.context = context;
    this.options = options;
    this.pluginRetriever = pluginRetriever;
    this.username = username;

    this.systemNamespaceService = context.getNamespaceService(SystemUser.SYSTEM_USERNAME);
    this.userNamespaceService = context.getNamespaceService(username);
    this.datasets = new DatasetManager(
      pluginRetriever,
      userNamespaceService
    );

    this.sourceModifier = sourceModifier;
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key){
    return datasets.getTable(key, options);
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    NamespaceKey resolved = resolveToDefault(key);
    if(resolved != null) {
      DremioTable t = datasets.getTable(resolved, options);
      if(t != null) {
        return t;
      }
    }

    return datasets.getTable(key, options);
  }

  @Override
  public DremioTable getTable(String datasetId) {
    return datasets.getTable(datasetId, options);
  }

  @Override
  public Iterable<DremioTable> getAllRequestedTables() {
    throw new UnsupportedOperationException("getAllRequestedTables() not supported on the default implementation");
  }

  @Override
  public NamespaceKey resolveSingle(NamespaceKey key) {
    if(getDefaultSchema() == null || key.size() > 1) {
      return key;
    }
    return getDefaultSchema().getChild(key.getLeaf());
  }

  @Override
  public boolean containerExists(NamespaceKey path) {

    final SchemaType type = getType(path, false);
    if(type == SchemaType.UNKNOWN) {
      return false;
    }

    try {
      List<NameSpaceContainer> containers = userNamespaceService.getEntities(ImmutableList.of(path));
      NameSpaceContainer c = containers.get(0);
      if(c != null &&
        (
          (c.getType() == NameSpaceContainer.Type.FOLDER && type != SchemaType.SOURCE) // DX-10186. Bad behavior in tryCreatePhysicalDataset causes problems with extra phantom folders.
            || c.getType() == NameSpaceContainer.Type.HOME
            || c.getType() == NameSpaceContainer.Type.SPACE
            || c.getType() == NameSpaceContainer.Type.SOURCE)) {
        return true;
      }

      if(type != SchemaType.SOURCE) {
        return false;
      }

      // For some sources, some folders aren't automatically existing in namespace, let's be more invasive...

      // let's check for a dataset in this path. We're looking for a dataset who either has this path as the schema of it or has a schema that starts with this path.
      if(!Iterables.isEmpty(userNamespaceService.find(new FindByCondition().setCondition(
        SearchQueryUtils.and(
          SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), NameSpaceContainer.Type.DATASET.getNumber()),
          SearchQueryUtils.or(
            SearchQueryUtils.newTermQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA, path.asLowerCase().toUnescapedString()),
            SearchQueryUtils.newWildcardQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName(), path.asLowerCase().toUnescapedString() + ".*")
          )
        )
      )))) {;
        return true;
      }

      // could be a filesystem, let's check the source directly (most expensive).
      ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRoot(), false);
      if(plugin == null) {
        // possible race condition where this plugin is no longer still registered.
        return false;
      }

      return plugin.unwrap(StoragePlugin.class).containerExists(path);
    } catch(NamespaceException ex) {
      return false;
    }
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    final SearchQuery filter = path.size() == 0
        ? null
        : SearchQueryUtils.newTermQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA, path.toUnescapedString().toLowerCase());
    return FluentIterable.from(InfoSchemaTable.SCHEMATA.<Schema>asIterable("N/A", username, context.getDatasetListing(), filter))
        .transform(new com.google.common.base.Function<Schema, String>() {

          @Override
          public String apply(Schema input) {
            return input.SCHEMA_NAME;
          }
        });
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    final SearchQuery filter = SearchQueryUtils.and(
      SearchQueryUtils.newTermQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA, path.toUnescapedString().toLowerCase()),
      SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), NameSpaceContainer.Type.DATASET.number)
    );

    return InfoSchemaTable.TABLES.<Table>asIterable("N/A", username, context.getDatasetListing(), filter);
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path) {
    NamespaceKey resolved = resolveSingle(path);

    if( resolved != null ) {
      if(containerExists(resolved.getParent())) {
        List<Function> functions = new ArrayList<>();
        Collection<Function> resolvedFunctions = getFunctionsInternal(resolved);
        functions.addAll(resolvedFunctions);
        return functions;
      }
    }

    List<Function> functions = new ArrayList<>();
    if(containerExists(path.getParent())) {
      functions.addAll(getFunctionsInternal(path));
    }
    return functions;
  }

  private Collection<Function> getFunctionsInternal(
    NamespaceKey path
  ) {

    switch (getType(path, true)) {
      case SOURCE:
        FileSystemPlugin plugin = asFSn(path);
        if(plugin == null) {
          return ImmutableList.of();
        }
        return plugin.getFunctions(path.getPathComponents(), options.getSchemaConfig());

      case HOME:

        try {
          return getHomeFilesPlugin().getFunctions(path.getPathComponents(), options.getSchemaConfig());
        } catch (ExecutionSetupException e) {
          throw new RuntimeException(e);
        }
      case SPACE:
      default:
        return Collections.emptyList();
    }
  }

  @Override
  public NamespaceKey getDefaultSchema() {
    return options.getSchemaConfig().getDefaultSchema();
  }

  @Override
  public String getUser() {
    return options.getSchemaConfig().getUserName();
  }

  @Override
  public NamespaceKey resolveToDefault(NamespaceKey key) {
    if(options.getSchemaConfig().getDefaultSchema() == null) {
      return null;
    }

    return new NamespaceKey(
      ImmutableList.copyOf(
        Iterables.concat(
          options.getSchemaConfig().getDefaultSchema().getPathComponents(),
          key.getPathComponents())));
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema) {
    return new CatalogImpl(context, options.cloneWith(username, newDefaultSchema), pluginRetriever, sourceModifier);
  }

  @Override
  public Catalog resolveCatalog(String username) {
    return new CatalogImpl(context, options.cloneWith(username, options.getSchemaConfig().getDefaultSchema()), pluginRetriever, sourceModifier);
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CatalogImpl(context, options.cloneWith(getUser(), newDefaultSchema), pluginRetriever, sourceModifier);
  }

  @Override
  public MetadataStatsCollector getMetadataStatsCollector() {
    return options.getStatsCollector();
  }

  private FileSystemPlugin getHomeFilesPlugin() throws ExecutionSetupException {
    return pluginRetriever.getPlugin("__home", true).unwrap(FileSystemPlugin.class);
  }

  @Override
  public CreateTableEntry createNewTable(
    final NamespaceKey key,
    final WriterOptions writerOptions,
    final Map<String, Object> storageOptions) {
    return asMutable(key, "does not support create table operations.").createNewTable(options.getSchemaConfig(), key, writerOptions, storageOptions);
  }

  @Override
  public void createView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    switch(getType(key, true)) {
      case SOURCE:
        asMutable(key, "does not support create view")
          .createOrUpdateView(key, view, options.getSchemaConfig());
        break;
      case SPACE:
      case HOME:
        context.getViewCreator(getUser())
          .createView(key.getPathComponents(), view.getSql(), Collections.<String>emptyList(), attributes);
        break;
      default:
        throw UserException.unsupportedError()
          .message("Cannot create view in %s.", key)
          .build(logger);
    }
  }

  @Override
  public void updateView(NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    switch(getType(key, true)) {
      case SOURCE:
        asMutable(key, "does not support update view")
          .createOrUpdateView(key, view, options.getSchemaConfig());
        break;
      case SPACE:
      case HOME:
        context.getViewCreator(getUser())
          .updateView(key.getPathComponents(), view.getSql(), view.getWorkspaceSchemaPath(), attributes);
        break;
      default:
        throw UserException.unsupportedError()
          .message("Cannot update view in %s.", key)
          .build(logger);
    }
  }

  @Override
  public void dropView(
    final NamespaceKey key
  ) throws IOException {
    switch (getType(key, true)) {
      case SOURCE:
        asMutable(key, "does not support view operations.").dropView(options.getSchemaConfig(), key.getPathComponents());
        return;
      case SPACE:
      case HOME:
        context.getViewCreator(getUser()).dropView(key.getPathComponents());
        return;
      default:
        throw UserException.unsupportedError().message("Invalid request to drop " + key).build(logger);
    }
  }

  /**
   * Determines which SchemaType the root of this path corresponds to.
   * @param key The key whose root we will evaluate.
   * @return The SchemaType associated with this key.
   */
  private SchemaType getType(NamespaceKey key, boolean throwOnMissing) {
    try {

      if(("@" + getUser()).equalsIgnoreCase(key.getRoot())) {
        return SchemaType.HOME;
      }

      NameSpaceContainer container = systemNamespaceService.getEntities(ImmutableList.of(new NamespaceKey(key.getRoot()))).get(0);
      if(container == null) {
        return SchemaType.UNKNOWN;
      }

      Type type = container.getType();
      switch(type) {
        case SOURCE:
          return SchemaType.SOURCE;
        case SPACE:
          return SchemaType.SPACE;

        case HOME:
          throw UserException.permissionError().message("No permission to requested path.").build(logger);

        default:
          throw new IllegalStateException("Unexpected container type: " + type.name());
      }
    } catch (NamespaceNotFoundException e) {
      if(throwOnMissing) {
        throw UserException.validationError().message("Unable to find requested path %s.", key).build(logger);
      } else {
        return SchemaType.UNKNOWN;
      }
    } catch (NamespaceException e) {
      throw Throwables.propagate(e);
    }
  }

  private FileSystemPlugin asFSn(NamespaceKey key) {
    final StoragePlugin plugin = context.getCatalogService().getSource(key.getRoot());
    if (plugin instanceof FileSystemPlugin) {
      return (FileSystemPlugin) plugin;
    } else {
      return null;
    }
  }

  private MutablePlugin asMutable(NamespaceKey key, String error) {
    StoragePlugin plugin = context.getCatalogService().getSource(key.getRoot());
    if (plugin instanceof MutablePlugin) {
      return (MutablePlugin) plugin;
    }

    throw new UnsupportedOperationException(key.getRoot() + " " + error);
  }

  @Override
  public void dropTable(NamespaceKey key) {
    asMutable(key, "does not support dropping tables").dropTable(key.getPathComponents(), options.getSchemaConfig());
    try {
      systemNamespaceService.deleteEntity(key);
    } catch (NamespaceException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void createDataset(NamespaceKey key, com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator) {
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", key.getRoot()).build(logger);
    }

    datasets.createDataset(key, plugin, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", key.getRoot()).build(logger);
    }

    return plugin.refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", key.getRoot()).build(logger);
    }

    plugin.refreshState();
    plugin.initiateMetadataRefresh();
    return plugin.getState();
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key,
      List<String> partitionColumns,
      List<String> partitionValues) throws PartitionNotFoundException {

    if(pluginRetriever.getPlugin(key.getRoot(), true) == null){
      throw UserException.validationError().message("Unknown source %s", key.getRoot()).build(logger);
    }

    StoragePlugin plugin = context.getCatalogService().getSource(key.getRoot());
    FileSystemPlugin fsPlugin;
    if (plugin instanceof FileSystemPlugin) {
      fsPlugin = (FileSystemPlugin) plugin;
      return fsPlugin.getSubPartitions(key.getPathComponents(), partitionColumns, partitionValues, options.getSchemaConfig());
    }

    throw new UnsupportedOperationException(plugin.getClass().getName() + " does not support partition retrieval.");
  }

  @Override
  public boolean createOrUpdateDataset(NamespaceService userNamespaceService, NamespaceKey source, NamespaceKey datasetPath, DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException {
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(source.getRoot(), true);
    if(plugin == null){
      throw UserException.validationError().message("Unknown source %s", datasetPath.getRoot()).build(logger);
    }

    return datasets.createOrUpdateDataset(userNamespaceService, plugin, source, datasetPath, datasetConfig, attributes);
  }

  @Override
  public <T extends StoragePlugin> T getSource(String name) {
    return sourceModifier.getSource(name);
  }

  @Override
  public void createSource(SourceConfig config, NamespaceAttribute... attributes) {
    sourceModifier.createSource(config, attributes);
  }

  @Override
  public void updateSource(SourceConfig config, NamespaceAttribute... attributes) {
    sourceModifier.updateSource(config, attributes);
  }

  @Override
  public void deleteSource(SourceConfig config) {
    sourceModifier.deleteSource(config);

  }

  @Override
  public boolean isSourceConfigMetadataImpacting(SourceConfig config) {
    return pluginRetriever.getPlugin(config.getName(), false).isSourceConfigMetadataImpacting(config);
  }

  @Override
  public SourceState getSourceState(String name) {
    // Preconditions.checkState(isCoordinator);
    ManagedStoragePlugin plugin = pluginRetriever.getPlugin(name, false);
    if(plugin == null) {
      return null;
    }
    return plugin.getState();
  }

  private enum SchemaType {
    SOURCE, SPACE, HOME, UNKNOWN
  }
}
