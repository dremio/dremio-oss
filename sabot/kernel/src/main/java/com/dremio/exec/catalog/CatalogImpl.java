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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.schema.Function;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.Serializer;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.options.OptionManager;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.DatasetIndexKeys;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetField;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ParentDataset;
import com.dremio.service.namespace.dataset.proto.VirtualDataset;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * Default, non caching, implementation of {@link Catalog}
 */
@ThreadSafe
public class CatalogImpl implements Catalog {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogImpl.class);
  private static final int MAX_RETRIES = 5;

  private final MetadataRequestOptions options;
  private final PluginRetriever pluginRetriever;
  private final CatalogServiceImpl.SourceModifier sourceModifier;
  private final String username;

  private final OptionManager optionManager;
  private final NamespaceService systemNamespaceService;
  private final NamespaceService.Factory namespaceFactory;
  private final DatasetListingService datasetListingService;
  private final ViewCreatorFactory viewCreatorFactory;

  private final NamespaceService userNamespaceService;
  private final DatasetManager datasets;
  private final InformationSchemaCatalog iscDelegate;

  private final Set<String> selectedSources;
  private final boolean crossSourceSelectDisable;

  CatalogImpl(
      MetadataRequestOptions options,
      PluginRetriever pluginRetriever,
      CatalogServiceImpl.SourceModifier sourceModifier,
      OptionManager optionManager,
      NamespaceService systemNamespaceService,
      NamespaceService.Factory namespaceFactory,
      DatasetListingService datasetListingService,
      ViewCreatorFactory viewCreatorFactory
      ) {
    this.options = options;
    this.pluginRetriever = pluginRetriever;
    this.sourceModifier = sourceModifier;
    this.username = options.getSchemaConfig().getUserName();

    this.optionManager = optionManager;
    this.systemNamespaceService = systemNamespaceService;
    this.namespaceFactory = namespaceFactory;
    this.datasetListingService = datasetListingService;
    this.viewCreatorFactory = viewCreatorFactory;

    this.userNamespaceService = namespaceFactory.get(username);

    this.datasets = new DatasetManager(pluginRetriever, userNamespaceService, optionManager);
    this.iscDelegate = new InformationSchemaCatalogImpl(userNamespaceService);

    this.selectedSources = ConcurrentHashMap.newKeySet();
    this.crossSourceSelectDisable = optionManager.getOption(CatalogOptions.DISABLE_CROSS_SOURCE_SELECT);
  }

  @Override
  public void addOrUpdateDataset(NamespaceKey datasetPath, DatasetConfig dataset) throws NamespaceException {
    userNamespaceService.addOrUpdateDataset(datasetPath, dataset);
  }

  @Override
  public DremioTable getTableNoResolve(NamespaceKey key)
  {
    final DremioTable t = datasets.getTable(key, options, false);
    if (t != null) {
      addUniqueSource(t);
    }
    return t;
  }

  @Override
  public DremioTable getTableNoColumnCount(NamespaceKey key) {
    final DremioTable t = datasets.getTable(key, options, true);
    if (t != null) {
      addUniqueSource(t);
    }
    return t;
  }

  @Override
  public DremioTable getTable(NamespaceKey key) {
    final NamespaceKey resolved = resolveToDefault(key);
    if (resolved != null) {
      final DremioTable t = datasets.getTable(resolved, options, false);
      if (t != null) {
        addUniqueSource(t);
        if (t instanceof ViewTable) {
          View view = ((ViewTable) t).getView();
          NamespaceKey viewPath = ((ViewTable) t).getPath();
          try {
            if (view.isFieldUpdated()) {
              updateView(viewPath, view);
            }
          } catch (Exception ex) {
            logger.warn("Failed to update view with updated nested schema: ", ex);
          }
        }
        return t;
      }
    }
    final DremioTable t = datasets.getTable(key, options, false);
    if (t != null) {
      addUniqueSource(t);
      if (t instanceof ViewTable) {
        View view = ((ViewTable) t).getView();
        NamespaceKey viewPath = ((ViewTable) t).getPath();
        try {
          if (view.isFieldUpdated()) {
            updateView(viewPath, view);
          }
        } catch (Exception ex) {
          logger.warn("Failed to update view with updated nested schema: ", ex);
        }
      }
    }
    return t;
  }

  @Override
  public DremioTable getTable(String datasetId) {
    final DremioTable t = datasets.getTable(datasetId, options);
    if (t != null) {
      addUniqueSource(t);
    }
    return t;
  }

  private void addUniqueSource(DremioTable t) {
    if (!crossSourceSelectDisable) {
      return;
    }
    if (t instanceof NamespaceTable) {
      selectedSources.add(((NamespaceTable) t).getDatasetConfig().getFullPathList().get(0));
    }
    else if (t instanceof ViewTable) {
      DatasetConfig ds = ((ViewTable) t).getDatasetConfig();
      if (ds != null) {
        expandSourceCheckParent(ds.getVirtualDataset().getParentsList());
        expandSourceCheckParent(ds.getVirtualDataset().getGrandParentsList());
      }
    }
  }

  private void expandSourceCheckParent(List<ParentDataset> ps) {
    if (ps != null) {
      for (ParentDataset pd : ps) {
        final String srcName = pd.getDatasetPathList().get(0);
        final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(srcName, false);
        if (plugin != null) {
          selectedSources.add(srcName);
        }
      }
    }
  }

  @Override
  public void validateSelection() {
    if ((this.crossSourceSelectDisable) && (selectedSources.size() > 1)) {
      final List<String> disallowedSources = new ArrayList<>();
      for (String s : selectedSources) {
        if (("sys".equalsIgnoreCase(s)) || ("INFORMATION_SCHEMA".equalsIgnoreCase(s))) {
          continue;
        }
        final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(s, false);
        if (!plugin.getConfig().getAllowCrossSourceSelection()) {
          disallowedSources.add(s);
        }
      }
      if (disallowedSources.size() > 1) {
        Collections.sort(disallowedSources);
        final String str = disallowedSources.stream()
          .collect(Collectors.joining("', '", "Cross select is disabled between sources '", "'."));
        throw UserException.validationError()
          .message(str)
          .buildSilently();
      }
    }
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
      if(!Iterables.isEmpty(userNamespaceService.find(new LegacyFindByCondition().setCondition(
        SearchQueryUtils.and(
          SearchQueryUtils.newTermQuery(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName(), NameSpaceContainer.Type.DATASET.getNumber()),
          SearchQueryUtils.or(
            SearchQueryUtils.newTermQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA, path.asLowerCase().toUnescapedString()),
            SearchQueryUtils.newPrefixQuery(DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName(),
              path.asLowerCase().toUnescapedString() + ".")
          )
        )
      )))) {
        return true;
      }

      // could be a filesystem, let's check the source directly (most expensive).
      ManagedStoragePlugin plugin = pluginRetriever.getPlugin(path.getRoot(), false);
      if(plugin == null) {
        // possible race condition where this plugin is no longer still registered.
        return false;
      }

      return plugin.unwrap(StoragePlugin.class).containerExists(new EntityPath(path.getPathComponents()));
    } catch(NamespaceException ex) {
      return false;
    }
  }

  @Override
  public Iterable<String> listSchemas(NamespaceKey path) {
    final SearchQuery searchQuery =
      path.size() == 0 ? null : SearchQuery.newBuilder()
        .setEquals(SearchQuery.Equals.newBuilder()
          .setField(DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName())
          .setStringValue(path.toUnescapedString().toLowerCase())
          .build())
        .build();
    final Iterable<Schema> iterable = () -> listSchemata(searchQuery);

    return FluentIterable.from(iterable)
        .transform(input -> input.getSchemaName());
  }

  @Override
  public Iterable<Table> listDatasets(NamespaceKey path) {
    final SearchQuery searchQuery =
      SearchQuery.newBuilder()
        .setAnd(SearchQuery.And.newBuilder()
          .addClauses(SearchQuery.newBuilder()
            .setEquals(SearchQuery.Equals.newBuilder()
              .setField(DatasetIndexKeys.UNQUOTED_LC_SCHEMA.getIndexFieldName())
              .setStringValue(path.toUnescapedString().toLowerCase())))
          .addClauses(SearchQuery.newBuilder()
            .setEquals(SearchQuery.Equals.newBuilder()
              .setField(NamespaceIndexKeys.ENTITY_TYPE.getIndexFieldName())
              .setIntValue(NameSpaceContainer.Type.DATASET.getNumber()))))
        .build();

    return () -> listTables(searchQuery);
  }

  @Override
  public Collection<Function> getFunctions(NamespaceKey path) {
    final NamespaceKey resolved = resolveSingle(path);

    if (resolved != null) {
      if (containerExists(resolved.getParent())) {
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
        return sourceModifier.getSource(path.getRoot()).getFunctions(path.getPathComponents(), options.getSchemaConfig());

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
  public Catalog resolveCatalog(boolean checkValidity) {
    return new CatalogImpl(
      options.cloneWith(options.getSchemaConfig().getUserName(), options.getSchemaConfig().getDefaultSchema(), checkValidity),
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema, boolean checkValidity) {
    return new CatalogImpl(
      options.cloneWith(username, newDefaultSchema, checkValidity),
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);
  }

  @Override
  public Catalog resolveCatalog(String username, NamespaceKey newDefaultSchema) {
    return new CatalogImpl(
      options.cloneWith(username, newDefaultSchema, options.checkValidity()),
      pluginRetriever,
      sourceModifier.cloneWith(username),
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);
  }

  @Override
  public Catalog resolveCatalog(String username) {
    return new CatalogImpl(
      options.cloneWith(username, options.getSchemaConfig().getDefaultSchema(), options.checkValidity()),
      pluginRetriever,
      sourceModifier.cloneWith(username),
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);
  }

  @Override
  public Catalog resolveCatalog(NamespaceKey newDefaultSchema) {
    return new CatalogImpl(
      options.cloneWith(username, newDefaultSchema, options.checkValidity()),
      pluginRetriever,
      sourceModifier.cloneWith(username),
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);
  }

  @Override
  public MetadataStatsCollector getMetadataStatsCollector() {
    return options.getStatsCollector();
  }

  private FileSystemPlugin getHomeFilesPlugin() throws ExecutionSetupException {
    return pluginRetriever.getPlugin("__home", true).unwrap(FileSystemPlugin.class);
  }

  @Override
  public void createEmptyTable(NamespaceKey key, BatchSchema batchSchema, final WriterOptions writerOptions) {
    asMutable(key, "does not support create table operations.").createEmptyTable(options.getSchemaConfig(), key,
      batchSchema, writerOptions);
  }

  @Override
  public CreateTableEntry createNewTable(
    final NamespaceKey key,
    final IcebergTableProps icebergTableProps,
    final WriterOptions writerOptions,
    final Map<String, Object> storageOptions) {
    return asMutable(key, "does not support create table operations.")
      .createNewTable(options.getSchemaConfig(), key, icebergTableProps, writerOptions, storageOptions);
  }

  @Override
  public void createView(final NamespaceKey key, View view, NamespaceAttribute... attributes) throws IOException {
    switch (getType(key, true)) {
      case SOURCE:
        asMutable(key, "does not support create view")
          .createOrUpdateView(key, view, options.getSchemaConfig());
        break;
      case SPACE:
      case HOME:
        if (view.hasDeclaredFieldNames()) {
          throw UserException.unsupportedError()
              .message("Dremio doesn't support field aliases defined in view creation.")
              .buildSilently();
        }
        viewCreatorFactory.get(username)
          .createView(key.getPathComponents(), view.getSql(), view.getWorkspaceSchemaPath(), attributes);
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
        viewCreatorFactory.get(username)
          .updateView(key.getPathComponents(), view.getSql(), view.getWorkspaceSchemaPath(), attributes);
        break;
      default:
        throw UserException.unsupportedError()
          .message("Cannot update view in %s.", key)
          .build(logger);
    }
  }

  @Override
  public void dropView(final NamespaceKey key) throws IOException {
    switch (getType(key, true)) {
      case SOURCE:
        asMutable(key, "does not support view operations.").dropView(options.getSchemaConfig(), key.getPathComponents());
        return;
      case SPACE:
      case HOME:
        viewCreatorFactory.get(username).dropView(key.getPathComponents());
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

      if(("@" + username).equalsIgnoreCase(key.getRoot())) {
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

  private MutablePlugin asMutable(NamespaceKey key, String error) {
    StoragePlugin plugin = sourceModifier.getSource(key.getRoot());
    if (plugin instanceof MutablePlugin) {
      return (MutablePlugin) plugin;
    }

    throw UserException.unsupportedError().message(key.getRoot() + " " + error).build(logger);
  }

  @Override
  public void dropTable(NamespaceKey key) {

    DatasetConfig dataset;
    try {
      dataset = systemNamespaceService.getDataset(key);
    } catch (NamespaceException ex) {
      throw new RuntimeException(ex);
    }

    if (userNamespaceService.hasChildren(key)) {
      throw UserException.validationError()
        .message("Cannot drop table [%s] since it has child tables ", key)
        .buildSilently();
    }

    boolean isLayered = DatasetHelper.isIcebergDataset(dataset);

    asMutable(key, "does not support dropping tables")
      .dropTable(key.getPathComponents(), isLayered, options.getSchemaConfig());

    try {
      systemNamespaceService.deleteEntity(key);
    } catch (NamespaceException e) {
      throw Throwables.propagate(e);
    }
  }

  private boolean isSystemTable(DatasetConfig config) {
    // check if system tables and information schema.
    final String root = config.getFullPathList().get(0);
    if( ("sys").equals(root) || ("INFORMATION_SCHEMA").equals(root) ) {
      return true;
    }
    return false;
  }

  private DatasetConfig getConfigFromNamespace(NamespaceKey key) {
    try {
      return userNamespaceService.getDataset(key);
    } catch(NamespaceNotFoundException ex) {
      return null;
    } catch(NamespaceException ex) {
      throw new RuntimeException(ex);
    }
  }

  private DatasetConfig getConfigByCanonicalKey(NamespaceKey key) {

    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), false);

    if (plugin == null) {
      return null;
    }

    final Optional<DatasetHandle> handle;
    try {
        handle = plugin.getDatasetHandle(key, null, plugin.getDefaultRetrievalOptions());
    } catch (ConnectorException e) {
        throw UserException.validationError(e)
          .message("Failure while retrieving dataset [%s].", key)
          .build(logger);
    }

    if (handle.isPresent()) {
      final NamespaceKey canonicalKey = MetadataObjectsUtils.toNamespaceKey(handle.get().getDatasetPath());
      if (!canonicalKey.equals(key)) {
        // before we do anything with this accessor, we should reprobe namespace as it is possible that the request the
        // user made was not the canonical key and therefore we missed when trying to retrieve data from the namespace.
        return getConfigFromNamespace(canonicalKey);
      }
    }  // handle is present

    return null;
  }

  @Override
  public void forgetTable(NamespaceKey key) {

    int count = 0;

    while (true) {
      DatasetConfig dataset;
      dataset = getConfigFromNamespace(key);

      if (dataset == null) {
        // try to check if canonical key finds
        dataset = getConfigByCanonicalKey(key);
      } // passed-in key not found in kv

      if(dataset == null || isSystemTable(dataset)) {
        throw UserException.parseError().message("Unable to find table %s.", key).build(logger);
      }

      try {
        userNamespaceService.deleteDataset(new NamespaceKey(dataset.getFullPathList()), dataset.getTag());
        break;
      } catch (NamespaceNotFoundException ex) {
        logger.debug("Table to delete not found", ex);
      } catch (ConcurrentModificationException ex) {
        if (count++ < MAX_RETRIES) {
          logger.debug("Concurrent failure.", ex);
        } else {
          throw ex;
        }
      } catch(NamespaceException ex) {
        throw new RuntimeException(ex);
      }
    } // while loop
  }

  @Override
  public void truncateTable(NamespaceKey key) {
    asMutable(key, "does not support truncating tables")
      .truncateTable(key, options.getSchemaConfig());
  }

  @Override
  public void addColumns(NamespaceKey key, List<Field> colsToAdd) {
    asMutable(key, "does not support schema update")
        .addColumns(key, colsToAdd, options.getSchemaConfig());
  }

  @Override
  public void dropColumn(NamespaceKey table, String columnToDrop) {
    asMutable(table, "does not support schema update")
        .dropColumn(table, columnToDrop, options.getSchemaConfig());
  }

  @Override
  public void changeColumn(NamespaceKey table, String columnToChange, Field fieldFromSql) {
    asMutable(table, "does not support schema update")
        .changeColumn(table, columnToChange, fieldFromSql, options.getSchemaConfig());
  }

  /**
   * Sets table properties and refreshes dataset if properties changed
   *
   * @param key
   * @param attributes
   * @return if dataset config is updated
   */
  @Override
  public boolean alterDataset(final NamespaceKey key, final Map<String, AttributeValue> attributes) {
    final DatasetConfig datasetConfig;
    try {
      datasetConfig = systemNamespaceService.getDataset(key);
      if (datasetConfig.getType() == DatasetType.VIRTUAL_DATASET) {
        boolean changed = updateOptions(datasetConfig.getVirtualDataset(), attributes);
        if (changed) {
          // user userNamespaceService so that only those with "CAN EDIT" permission can make the change
          userNamespaceService.addOrUpdateDataset(key, datasetConfig);
        }
        return changed;
      }
    } catch (NamespaceException | ConcurrentModificationException ex) {
      throw UserException.validationError(ex)
        .message("Failure while accessing dataset")
        .buildSilently();
    }
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
                         .message("Unknown source [%s]", key.getRoot())
                         .buildSilently();
    }
    return plugin.alterDataset(key, datasetConfig, attributes);
  }

  private boolean updateOptions(VirtualDataset virtualDataset, Map<String, AttributeValue> attributes) {
    boolean changed = false;
    for (Entry<String,AttributeValue> attribute : attributes.entrySet()) {
      switch (attribute.getKey().toLowerCase()) {
      case "enable_default_reflection":
        AttributeValue.BooleanValue value = (AttributeValue.BooleanValue) attribute.getValue();
        boolean oldValue = Optional.ofNullable(virtualDataset.getDefaultReflectionEnabled()).orElse(true);
        if (value.getValue() != oldValue) {
          changed = true;
          virtualDataset.setDefaultReflectionEnabled(value.getValue());
        }
        break;
      default:
        throw UserException.validationError()
          .message("Unknown option [%s]", attribute.getKey())
          .buildSilently();
      }
    }
    return changed;
  }

  @Override
  public void createDataset(
      NamespaceKey key,
      com.google.common.base.Function<DatasetConfig, DatasetConfig> datasetMutator
  ) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }

    datasets.createDataset(key, plugin, datasetMutator);
  }

  @Override
  public UpdateStatus refreshDataset(NamespaceKey key, DatasetRetrievalOptions retrievalOptions) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }

    return plugin.refreshDataset(key, retrievalOptions);
  }

  @Override
  public SourceState refreshSourceStatus(NamespaceKey key) throws Exception {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(key.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }
    return plugin.refreshState().get();
  }

  @Override
  public Iterable<String> getSubPartitions(
      NamespaceKey key,
      List<String> partitionColumns,
      List<String> partitionValues
  ) throws PartitionNotFoundException {

    if (pluginRetriever.getPlugin(key.getRoot(), true) == null) {
      throw UserException.validationError()
          .message("Unknown source %s", key.getRoot())
          .buildSilently();
    }

    final StoragePlugin plugin = sourceModifier.getSource(key.getRoot());
    FileSystemPlugin fsPlugin;
    if (plugin instanceof FileSystemPlugin) {
      fsPlugin = (FileSystemPlugin) plugin;
      return fsPlugin.getSubPartitions(key.getPathComponents(), partitionColumns, partitionValues, options.getSchemaConfig());
    }

    throw new UnsupportedOperationException(plugin.getClass().getName() + " does not support partition retrieval.");
  }

  @Override
  public boolean createOrUpdateDataset(
      NamespaceService userNamespaceService,
      NamespaceKey source,
      NamespaceKey datasetPath,
      DatasetConfig datasetConfig,
      NamespaceAttribute... attributes
  ) throws NamespaceException {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(source.getRoot(), true);
    if (plugin == null) {
      throw UserException.validationError()
          .message("Unknown source %s", datasetPath.getRoot())
          .buildSilently();
    }

    return datasets.createOrUpdateDataset(plugin, datasetPath, datasetConfig, attributes);
  }


  @Override
  public void updateDatasetSchema(NamespaceKey datasetKey, BatchSchema newSchema) {
    final ManagedStoragePlugin plugin = pluginRetriever.getPlugin(datasetKey.getRoot(), true);

    boolean success;
    try {
      do {
        DatasetConfig oldConfig = systemNamespaceService.getDataset(datasetKey);
        DatasetConfig newConfig = plugin.getUpdatedDatasetConfig(oldConfig, newSchema);
        success = storeSchema(datasetKey, newConfig);
      } while (!success);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateDatasetField(NamespaceKey datasetKey, String originField, CompleteType fieldSchema) {
    boolean success;
    try {
      do {
        DatasetConfig oldDatasetConfig = systemNamespaceService.getDataset(datasetKey);

        Serializer<DatasetConfig, byte[]> serializer = ProtostuffSerializer.of(DatasetConfig.getSchema());
        DatasetConfig newDatasetConfig = serializer.deserialize(serializer.serialize(oldDatasetConfig));

        List<DatasetField> datasetFields = newDatasetConfig.getDatasetFieldsList();
        if (datasetFields == null) {
          datasetFields = Lists.newArrayList();
        }

        DatasetField datasetField = Iterables.find(datasetFields, new Predicate<DatasetField>() {
          @Override
          public boolean apply(@Nullable DatasetField input) {
            return originField.equals(input.getFieldName());
          }
        }, null);

        if (datasetField == null) {
          datasetField = new DatasetField().setFieldName(originField);
          datasetFields.add(datasetField);
        }

        datasetField.setFieldSchema(ByteString.copyFrom(fieldSchema.serialize()));
        newDatasetConfig.setDatasetFieldsList(datasetFields);

        success = storeSchema(datasetKey, newDatasetConfig);
      } while (!success);
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean storeSchema(NamespaceKey key, DatasetConfig config) throws NamespaceException {
    try {
      systemNamespaceService.addOrUpdateDataset(key, config);
      return true;
    } catch (ConcurrentModificationException ex) {
      return false;
    }
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

  private enum SchemaType {
    SOURCE, SPACE, HOME, UNKNOWN
  }

  @Override
  public Iterator<com.dremio.service.catalog.Catalog> listCatalogs(SearchQuery searchQuery) {
    return iscDelegate.listCatalogs(searchQuery);
  }

  @Override
  public Iterator<Schema> listSchemata(SearchQuery searchQuery) {
    return iscDelegate.listSchemata(searchQuery);
  }

  @Override
  public Iterator<com.dremio.service.catalog.Table> listTables(SearchQuery searchQuery) {
    return iscDelegate.listTables(searchQuery);
  }

  @Override
  public Iterator<com.dremio.service.catalog.View> listViews(SearchQuery searchQuery) {
    return iscDelegate.listViews(searchQuery);
  }

  @Override
  public Iterator<TableSchema> listTableSchemata(SearchQuery searchQuery) {
    return iscDelegate.listTableSchemata(searchQuery);
  }
}
