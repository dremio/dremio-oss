/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static com.dremio.exec.store.StoragePluginRegistryImpl.isInternal;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaTreeProvider.SchemaType;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.exec.util.ImpersonationUtil;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class SimpleSchema extends AbstractSchema {

  /*
   * Pair of MetadataPolicy plus last time this policy was updated (in milliseconds, originally set by System.currentTimeMillis())
   */
  public static class MetadataParam {
    private MetadataPolicy metadataPolicy;
    private Long lastMetadataRefreshDate;

    MetadataParam(MetadataPolicy metadataPolicy, Long lastMetadataRefreshDate) {
      this.metadataPolicy = metadataPolicy;
      this.lastMetadataRefreshDate = lastMetadataRefreshDate;
    }

    MetadataPolicy getMetadataPolicy() {
      return metadataPolicy;
    }
    Long getLastMetadataRefreshDate() {
      return lastMetadataRefreshDate == null ? 0 : lastMetadataRefreshDate;
    }
  }

  private static final long MAXIMUM_CACHE_SIZE = 10_000L;

  private static final PermissionCheckCache permissionsCache = new PermissionCheckCache(MAXIMUM_CACHE_SIZE);
  // Stores the time (in milliseconds, obtained from System.currentTimeMillis()) at which a dataset was locally updated
  private static final Cache<NamespaceKey, Long> localUpdateTime =
    CacheBuilder.newBuilder()
    .maximumSize(MAXIMUM_CACHE_SIZE)
    .build();

  private final SabotContext dContext;
  private final MetadataStatsCollector metadataStatsCollector;
  protected final SchemaConfig schemaConfig;
  protected final SchemaType type;
  protected final NamespaceService ns;
  private final MetadataParam metadataParam;
  private final SchemaMutability schemaMutability;

  private boolean areEntitiesListed;
  private Map<String,SimpleSchema> subSchemas;
  private Set<String> tableNames;

  public enum MetadataAccessType {
    CACHED_METADATA,
    PARTIAL_METADATA,
    SOURCE_METADATA
  }

  public SimpleSchema(
      SabotContext dContext,
      NamespaceService ns,
      MetadataStatsCollector metadataStatsCollector,
      List<String> parentSchemaPath,
      String name,
      SchemaConfig schemaConfig,
      SchemaType type,
      MetadataParam metadataParam,
      SchemaMutability schemaMutability) {
    super(parentSchemaPath, name);
    this.dContext = dContext;
    this.ns = ns;
    this.schemaConfig = schemaConfig;
    this.metadataStatsCollector = metadataStatsCollector;
    this.type = type;
    this.metadataParam = metadataParam;
    this.schemaMutability = Preconditions.checkNotNull(schemaMutability);
  }

  @Override
  public Schema getSubSchema(String name) {
    Schema subSchema = null;
    // If we have already fetched all entities under this schema, then reuse that list. Otherwise probe the namespace
    // just for the given subSchema name
    if (areEntitiesListed) {
      subSchema = subSchemas.get(name);
    } else if(ns.exists(new NamespaceKey(getChildPath(name)), Type.FOLDER)) {
      subSchema = new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, name, schemaConfig, type, metadataParam, schemaMutability);
    }

    if (subSchema != null) {
      return subSchema;
    }

    try {
      if (type == SchemaType.SOURCE){
        StoragePlugin plugin2 = dContext.getStorage().getPlugin(schemaPath.get(0));
        if(plugin2 != null && plugin2.containerExists(new NamespaceKey(getChildPath(name)))){
          return new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, name, schemaConfig, type, metadataParam, schemaMutability);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  @Override
  public SchemaMutability getMutability() {
    return schemaMutability;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    loadEntitiesUnderSchema();
    return subSchemas.keySet();
  }

  @Override
  public Set<String> getTableNames() {
    loadEntitiesUnderSchema();
    return tableNames;
  }

  /**
   * Helper method which loads the entities under the schema. Entities can be folders (subschemas) and tables.
   *
   * IMPORTANT: For performance reasons delay the loading of all entities until when
   * all subschema names {@link #getSubSchemaNames()} or all table names {@link #getTableNames()}
   * under the schema are requested. Do not try to load all entities when just a single subschem or table is
   * requested
   */
  private void loadEntitiesUnderSchema() {
    if (areEntitiesListed) {
      return;
    }

    this.subSchemas = Maps.newHashMap();
    this.tableNames = Sets.newHashSet();
    try {
      List<NameSpaceContainer> containers = ns.list(new NamespaceKey(schemaPath));
      for (NameSpaceContainer container : containers) {
        switch (container.getType()) {
          case DATASET:
            tableNames.add(container.getDataset().getName());
            break;
          case FOLDER:
            final String folderName = container.getFolder().getName();
            subSchemas.put(
                folderName,
                new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, folderName, schemaConfig, type, metadataParam, schemaMutability)
            );
            break;
          default:
        }
      }
    } catch (NamespaceNotFoundException ex) {
    } catch (NamespaceException ex) {
      // can't recover. propagate the exception
      Throwables.propagate(ex);
    }

    areEntitiesListed = true;
  }

  public void exposeSubSchemasAsTopLevelSchemas(SchemaPlus rootSchema) {
    loadEntitiesUnderSchema();
    if (schemaPath.size() > 1) {
      SubSchemaWrapper wrapper = new SubSchemaWrapper(this);
      rootSchema.add(wrapper.getName(), wrapper);
    }
    for (SimpleSchema subSchema : subSchemas.values()) {
      subSchema.exposeSubSchemasAsTopLevelSchemas(rootSchema);
    }
  }

  @Override
  public boolean showInInformationSchema() {
    String sourceName = schemaPath.get(0);
    return !isInternal(sourceName);
  }

  @Override
  public Iterable<String> getSubPartitions(String table,
      List<String> partitionColumns,
      List<String> partitionValues
  ) throws PartitionNotFoundException {
    if (type == SchemaType.SOURCE) {
      try {

        FileSystemPlugin plugin2 = (FileSystemPlugin) dContext.getStorage().getPlugin(schemaPath.get(0));

        return plugin2.getSubPartitions(
            getChildPath(table),
            partitionColumns,
            partitionValues,
            schemaConfig
        );
      } catch (ExecutionSetupException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Collections.emptyList();
    }
  }

  private boolean isPartialState(DatasetConfig config) {
    return DatasetHelper.getSchemaBytes(config) == null || config.getReadDefinition() == null;
  }

  private Table getTableWithRegistry(StoragePlugin registry, NamespaceKey key) {
    final DatasetConfig datasetConfig = getDataset(ns, key, metadataParam);
    if (datasetConfig != null) {
      return getTableFromDataset(registry, datasetConfig);
    }
    return getTableFromSource(registry, key);
  }

  private Table getTableFromDataset(StoragePlugin registry, DatasetConfig datasetConfig) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    // we assume for now that the passed dataset was retrieved from the namespace, so it must have
    // a canonized datasetPath (or the source is case sensitive and doesn't really do any special handling
    // to canonize the keys)
    final NamespaceKey canonicalKey = new NamespaceKey(datasetConfig.getFullPathList());

    if (!permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey,
        datasetConfig, metadataParam == null ? null : metadataParam.getMetadataPolicy(), metadataStatsCollector)) {
      throw UserException.permissionError()
        .message("Access denied reading dataset %s.", canonicalKey.toString())
        .build(logger);
    }

    if (!isPartialState(datasetConfig)) {
      final NamespaceTable namespaceTable = new NamespaceTable(new TableMetadataImpl(registry.getId(), datasetConfig, schemaConfig.getUserName(), new SplitsPointerImpl(datasetConfig, ns)));
      stopwatch.stop();
      metadataStatsCollector.addDatasetStat(canonicalKey.getSchemaPath(), MetadataAccessType.CACHED_METADATA.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return namespaceTable;
    }

    try {
      // even though the following call won't assume the key is canonical we will need to hit the source anyway
      // to retrieve the complete dataset
      //TODO maybe I should introduce isCanonical field in the NamespaceKey
      SourceTableDefinition datasetAccessor = registry.getDataset(
        canonicalKey,
        datasetConfig,
        schemaConfig.getIgnoreAuthErrors()
      );

      final DatasetConfig newDatasetConfig = datasetAccessor.getDataset();
      NamespaceUtils.copyFromOldConfig(datasetConfig, newDatasetConfig);
      List<DatasetSplit> splits = datasetAccessor.getSplits();
      // Update the dataset using the system user. The current user may not have permissions
      // to update datasets.
      setDataset(dContext.getNamespaceService(SYSTEM_USERNAME), datasetAccessor.getName(), newDatasetConfig, splits);
      // check permission again.
      if (permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey,
          newDatasetConfig, metadataParam == null ? null : metadataParam.getMetadataPolicy(), metadataStatsCollector)) {
        TableMetadata metadata = new TableMetadataImpl(registry.getId(), newDatasetConfig, schemaConfig.getUserName(), new SplitsPointerImpl(splits, splits.size()));
        stopwatch.stop();
        metadataStatsCollector.addDatasetStat(canonicalKey.getSchemaPath(), MetadataAccessType.PARTIAL_METADATA.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return new NamespaceTable(metadata);
      } else {
        throw UserException.permissionError().message("Access denied reading dataset %s.", canonicalKey.toString()).build(logger);
      }
    } catch (Exception e) {
      throw UserException.dataReadError(e).message("Failure while attempting to read metadata for %s.%s.",  getFullSchemaName(), canonicalKey.getName()).build(logger);
    }
  }

  private Table getTableFromSource(StoragePlugin registry, NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      // TODO: move views to namespace and out of filesystem.
      ViewTable view = registry.getView(key.getPathComponents(), schemaConfig);
      if(view != null){
        return view;
      }

      // Get the old dataset, it could be expired but we need it for format settings when fetching the new metadata.
      final DatasetConfig oldDatasetConfig = getDataset(ns, key, null /* we are ok with expired dataset*/);
      final SourceTableDefinition accessor = registry.getDataset(key, oldDatasetConfig, schemaConfig.getIgnoreAuthErrors());

      if (accessor == null) {
        return null;
      }

      final NamespaceKey canonicalKey = accessor.getName(); // retrieve the canonical key returned by the source

      // Search the namespace using the canonical name. A table can be referred using multiple keys and namespace may
      // not contain entries for all keys
      final DatasetConfig datasetConfig = getDataset(ns, canonicalKey, metadataParam);
      if (datasetConfig != null) {
        return getTableFromDataset(registry, datasetConfig);
      }

      final DatasetConfig config = accessor.getDataset();
      final List<DatasetSplit> splits = accessor.getSplits();
      if(accessor.isSaveable()){
        try {
          if (ns.exists(canonicalKey)) {
            DatasetConfig origConfig = ns.getDataset(canonicalKey);
            NamespaceUtils.copyFromOldConfig(origConfig, config);
          }
          // Update the dataset using the system user. The current user may not have permissions
          // to update datasets.
          setDataset(dContext.getNamespaceService(SYSTEM_USERNAME), canonicalKey, config, splits);
        } catch (ConcurrentModificationException cme) {
          return getTableWithRegistry(registry, canonicalKey);
        } catch (UserException ue) {
          if (ue.getErrorType() == UserBitShared.DremioPBError.ErrorType.VALIDATION) {
            return getTableWithRegistry(registry, canonicalKey);
          }
          throw ue;
        }
      }
      if (permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey, config, metadataParam == null ? null : metadataParam.getMetadataPolicy(), metadataStatsCollector)) {
        final NamespaceTable namespaceTable = new NamespaceTable(new TableMetadataImpl(registry.getId(), config, schemaConfig.getUserName(), new SplitsPointerImpl(splits, splits.size())));
        stopwatch.stop();
        metadataStatsCollector.addDatasetStat(canonicalKey.getSchemaPath(), MetadataAccessType.SOURCE_METADATA.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        return namespaceTable;
      } else {
        throw UserException.permissionError().message("Access denied reading dataset %s.", canonicalKey.toString()).build(logger);
      }

    }catch(Exception ex){
      throw UserException.planError(ex).message("Failure while retrieving metadata for table %s.", key).build(logger);
    }
  }

  @Override
  public Table getTable(String name){
    final List<String> fullPathList = getChildPath(name);
    final NamespaceKey key = new NamespaceKey(fullPathList);
    final StoragePlugin registry = dContext.getCatalogService().getStoragePlugin(fullPathList.get(0));
    if(registry != null){
      return getTableWithRegistry(registry, key);
    }

    DatasetConfig datasetConfig = getDataset(ns, key, metadataParam);
    StoragePlugin plugin = null;
    switch (type) {
      case SPACE:
        if (datasetConfig == null) {
          return null;
        }
        return createTableFromVirtualDataset(datasetConfig, name);
      case HOME:
        if (datasetConfig == null) {
          return null;
        }
        switch (datasetConfig.getType()) {
          case VIRTUAL_DATASET:
            return createTableFromVirtualDataset(datasetConfig, name);
          case PHYSICAL_DATASET_HOME_FILE:
          case PHYSICAL_DATASET_HOME_FOLDER:
            try {
              return getTableFromDataset(getHomeFilesPlugin(), datasetConfig);
            } catch (ExecutionSetupException e) {
              throw new RuntimeException(e);
            }
          default:
            return null;
        }
      case SOURCE:
        try {
          String sourceName = schemaPath.get(0);
          plugin = dContext.getStorage().getPlugin(sourceName);
        } catch (ExecutionSetupException e) {
          throw new RuntimeException(e);
        }
        break;
    }
    if (plugin != null && (datasetConfig == null || DatasetHelper.getSchemaBytes(datasetConfig) == null)) {
      if (type == SchemaType.SOURCE) {
        // first check for view
        ViewTable viewTable = plugin.getView(fullPathList, schemaConfig);
        if (viewTable != null) {
          return viewTable;
        }
      }

    }

    return null;
  }

  private FileSystemPlugin getHomeFilesPlugin() throws ExecutionSetupException {
    return (FileSystemPlugin) dContext.getStorage().getPlugin("__home");
  }

  private ViewTable createTableFromVirtualDataset(DatasetConfig datasetConfig, String tableName) {
    try {
      View view = Views.fieldTypesToView(
          tableName,
          datasetConfig.getVirtualDataset().getSql(),
          ViewFieldsHelper.getCalciteViewFields(datasetConfig),
          datasetConfig.getVirtualDataset().getContextList()
      );
      return new ViewTable(view, datasetConfig.getOwner(), schemaConfig.getViewExpansionContext());
    } catch (Exception e) {
      logger.warn("Failure parsing virtual dataset, not including in available schema.", e);
      return null;
    }
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    switch (type) {
      case SOURCE:
        FileSystemPlugin plugin = asFSn();
        if(plugin == null) {
          return ImmutableList.of();
        }
        return plugin.getFunctions(getChildPath(name), schemaConfig);

      case HOME:

        try {
          return getHomeFilesPlugin().getFunctions(getChildPath(name), schemaConfig);
        } catch (ExecutionSetupException e) {
          throw new RuntimeException(e);
        }
      case SPACE:
      default:
        return Collections.emptyList();
    }
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public boolean createView(View view) throws IOException {
    switch(type) {
    case SOURCE:
      return asFS("dow not support create view").createView(getChildPath(view.getName()), view, schemaConfig);
    case SPACE:
    case HOME:
      String userName = schemaConfig.getAuthContext().getUsername();
      dContext.getViewCreator(userName).createView(getChildPath(view.getName()), view.getSql(), Collections.<String>emptyList());
      return true;
    default:
      throw UserException.unsupportedError().message("Cannot create view in root schema type: " + type).build(logger);
    }
  }

  @Override
  public void dropView(String viewName) throws IOException {
    switch (type) {
    case SOURCE:
      asFS(" does not support view operations.").dropView(schemaConfig, getChildPath(viewName));
      return;
    case SPACE:
    case HOME:
      String userName = schemaConfig.getAuthContext().getUsername();
      dContext.getViewCreator(userName).dropView(getChildPath(viewName));
      return;
    default:
      throw UserException.unsupportedError().message("Cannot drop view in root schema type: " + type).build(logger);
    }
  }

  @Override
  public CreateTableEntry createNewTable(
      final String tableName,
      final WriterOptions options,
      final Map<String, Object> storageOptions) {
    Preconditions.checkState(type == SchemaType.SOURCE);
    final FileSystemPlugin fsPlugin = asFS("does not support CTAS.");

    final FormatPlugin formatPlugin;
    if (storageOptions == null || storageOptions.isEmpty()) {
      String storage = schemaConfig.getOption(ExecConstants.OUTPUT_FORMAT_OPTION).string_val;
      formatPlugin = fsPlugin.getFormatPlugin(storage);
      if (formatPlugin == null) {
        throw new UnsupportedOperationException(
            String.format("Unsupported format '%s' in workspace '%s'", storage,
                Joiner.on(".").join(getSchemaPath())
            ));
      }
    } else {
      final FormatPluginConfig formatConfig = fsPlugin.createConfigForTable(tableName, storageOptions);
      formatPlugin = fsPlugin.getFormatPlugin(formatConfig);
    }

    String userName = fsPlugin.getId().<FileSystemConfig>getConfig().isImpersonationEnabled() ? schemaConfig.getUserName() : ImpersonationUtil.getProcessUserName();
    return new FileSystemCreateTableEntry(
        userName,
        fsPlugin,
        formatPlugin,
        fsPlugin.resolveTablePathToValidPath(tableName).toString(),
        options);
  }

  public FileSystemWrapper getFileSystem() {
    return asFS("does not support FileSystem operations").getFS(schemaConfig.getUserName());
  }

  public String resolveLocation(String location) {
    return asFS("does not support FileSystem operations").resolveTablePathToValidPath(location).toString();
  }

  @Override
  public void dropTable(String tableName) {
    asFS("does not support dropping tables").dropTable(getChildPath(tableName), schemaConfig);
  }

  private FileSystemPlugin asFSn() {
    try {
      final StoragePlugin plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
      if (plugin instanceof FileSystemPlugin) {
        return (FileSystemPlugin) plugin;
      } else {
        return null;
      }
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
  }

  private FileSystemPlugin asFS(String error) {
    try {
      StoragePlugin plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
      FileSystemPlugin fsPlugin;
      if (plugin instanceof FileSystemPlugin) {
        fsPlugin = (FileSystemPlugin) plugin;
      } else {
        throw new UnsupportedOperationException(plugin.getClass().getName() + " " + error);
      }
      return fsPlugin;
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
  }

  private static DatasetConfig getDataset(NamespaceService ns, NamespaceKey key, MetadataParam metadataParam) {
    try {
      Long updateTime = localUpdateTime.getIfPresent(key);
      long currentTime = System.currentTimeMillis();
      long expiryTime = metadataParam == null ? 0 : metadataParam.metadataPolicy.getDatasetDefinitionExpireAfterMs();
      // Check for expired entries. An entry is expired if:
      if (metadataParam != null &&                                               // it's a dataset that
        (updateTime == null || updateTime + expiryTime < currentTime) &&         // was locally updated too long ago (or never), AND
        metadataParam.getLastMetadataRefreshDate() + expiryTime < currentTime) { // was globally updated too long ago
        return null;
      }
      return ns.getDataset(key);
    } catch (NamespaceNotFoundException e) {
      return null;
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private static void setDataset(NamespaceService ns, NamespaceKey nsKey, DatasetConfig config, List<DatasetSplit> splits)  throws NamespaceException {
    ns.addOrUpdateDataset(nsKey, config, splits);
    localUpdateTime.put(nsKey, System.currentTimeMillis());
  }

  private List<String> getChildPath(final String childName) {
    return ImmutableList.<String>builder().addAll(schemaPath).add(childName).build();
  }

  @Override
  public String getTypeName() {
    return "simple";
  }
}
