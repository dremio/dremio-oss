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
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaTreeProvider.SchemaType;
import com.dremio.exec.store.SchemaTreeProvider.MetadataStatsCollector;
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
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.TableInstance.TableParamDef;
import com.dremio.service.namespace.TableInstance.TableSignature;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.proto.NameSpaceContainer.Type;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class SimpleSchema extends AbstractSchema {

  private static final CharMatcher PATH_SEPARATOR_MATCHER = CharMatcher.is(Path.SEPARATOR_CHAR);
  private static final PermissionCheckCache permissionsCache = new PermissionCheckCache(10_000L);

  private final SabotContext dContext;
  private final MetadataStatsCollector metadataStatsCollector;
  protected final SchemaConfig schemaConfig;
  protected final SchemaType type;
  protected final NamespaceService ns;
  private final MetadataPolicy metadataPolicy;
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
      MetadataPolicy metadataPolicy,
      SchemaMutability schemaMutability) {
    super(parentSchemaPath, name);
    this.dContext = dContext;
    this.ns = ns;
    this.schemaConfig = schemaConfig;
    this.metadataStatsCollector = metadataStatsCollector;
    this.type = type;
    this.metadataPolicy = metadataPolicy;
    this.schemaMutability = Preconditions.checkNotNull(schemaMutability);
  }

  public static String removeLeadingSlash(String path) {
    return PATH_SEPARATOR_MATCHER.trimLeadingFrom(path);
  }

  @Override
  public Schema getSubSchema(String name) {
    Schema subSchema = null;
    // If we have already fetched all entities under this schema, then reuse that list. Otherwise probe the namespace
    // just for the given subSchema name
    if (areEntitiesListed) {
      subSchema = subSchemas.get(name);
    } else if(ns.exists(new NamespaceKey(getChildPath(name)), Type.FOLDER)) {
      subSchema = new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, name, schemaConfig, type, metadataPolicy, schemaMutability);
    }

    if (subSchema != null) {
      return subSchema;
    }

    try {
      if (type == SchemaType.SOURCE){
        StoragePlugin<?> plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
        StoragePlugin2 plugin2 = plugin.getStoragePlugin2();
        if(plugin2 != null){
          if(plugin2.containerExists(new NamespaceKey(getChildPath(name)))){
            return new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, name, schemaConfig, type, metadataPolicy, schemaMutability);
          }
        }else{
          if(plugin.folderExists(schemaConfig, getChildPath(name))){
            return new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, name, schemaConfig, type, metadataPolicy, schemaMutability);
          }
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
                new SimpleSchema(dContext, ns, metadataStatsCollector, schemaPath, folderName, schemaConfig, type, metadataPolicy, schemaMutability)
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
        return dContext.getStorage().getPlugin(schemaPath.get(0)).getSubPartitions(
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

  private Table getTableWithRegistry(StoragePlugin2 registry, NamespaceKey key) {
    final DatasetConfig datasetConfig = getDataset(ns, key);
    if (datasetConfig != null) {
      return getTableFromDataset(registry, datasetConfig);
    }
    return getTableFromSource(registry, key);
  }

  private Table getTableFromDataset(StoragePlugin2 registry, DatasetConfig datasetConfig) {
    Stopwatch stopwatch = Stopwatch.createStarted();

    // we assume for now that the passed dataset was retrieved from the namespace, so it must have
    // a canonized datasetPath (or the source is case sensitive and doesn't really do any special handling
    // to canonize the keys)
    final NamespaceKey canonicalKey = new NamespaceKey(datasetConfig.getFullPathList());

    if (!permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey,
        datasetConfig, metadataPolicy, metadataStatsCollector)) {
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
      CatalogServiceImpl.copyFromOldConfig(datasetConfig, newDatasetConfig);
      List<DatasetSplit> splits = datasetAccessor.getSplits();
      ns.addOrUpdateDataset(datasetAccessor.getName(), newDatasetConfig, splits);
      // check permission again.
      if (permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey,
          newDatasetConfig, metadataPolicy, metadataStatsCollector)) {
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

  private Table getTableFromSource(StoragePlugin2 registry, NamespaceKey key) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      // TODO: move views to namespace and out of filesystem.
      ViewTable view = registry.getView(key.getPathComponents(), schemaConfig);
      if(view != null){
        return view;
      }

      final SourceTableDefinition accessor = registry.getDataset(key, null, schemaConfig.getIgnoreAuthErrors());

      if (accessor == null) {
        return null;
      }

      final NamespaceKey canonicalKey = accessor.getName(); // retrieve the canonical key returned by the source

      if (ns.exists(canonicalKey, Type.DATASET)) {
        // Some other thread may have added this dataset, so return from namespace with the canonical path
        return getTableWithRegistry(registry, canonicalKey);
      }

      final DatasetConfig config = accessor.getDataset();
      final List<DatasetSplit> splits = accessor.getSplits();
      if(accessor.isSaveable()){
        try {
          ns.addOrUpdateDataset(canonicalKey, config, splits);
        } catch (ConcurrentModificationException cme) {
          return getTableWithRegistry(registry, canonicalKey);
        } catch (UserException ue) {
          if (ue.getErrorType() == UserBitShared.DremioPBError.ErrorType.VALIDATION) {
            return getTableWithRegistry(registry, canonicalKey);
          }
          throw ue;
        }
      }
      if (permissionsCache.hasAccess(registry, schemaConfig.getUserName(), canonicalKey, config, metadataPolicy, metadataStatsCollector)) {
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
    final StoragePlugin2 registry = dContext.getCatalogService().getStoragePlugin(fullPathList.get(0));
    if(registry != null){
      return getTableWithRegistry(registry, key);
    }

    DatasetConfig datasetConfig = getDataset(ns, key);
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
              return getTableFromDataset(getHomeFilesPlugin().getStoragePlugin2(), datasetConfig);
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
      try {
        if (type == SchemaType.SOURCE) {
          // first check for view
          ViewTable viewTable = plugin.getView(fullPathList, schemaConfig);
          if (viewTable != null) {
            return viewTable;
          }
        }
        datasetConfig = plugin.getDataset(fullPathList, new TableInstance(new TableSignature(name, Collections.<TableParamDef>emptyList()), Collections.emptyList()), schemaConfig);
        if (datasetConfig == null) {
          return null;
        }
        if (type == SchemaType.SOURCE) {
          BatchSchema schema = BatchSchema.fromDataset(datasetConfig);
          if (schema.isUnknownSchema()) {
            // Don't store faked schemas
            logger.debug("Dataset, " + fullPathList + ", has unknown schema.  The dataset does not have any data to sample.");
          } else {
            ns.tryCreatePhysicalDataset(key, datasetConfig);
          }
        }
      } catch (NamespaceException e) {
        logger.warn("Exception", e);
      }
    }
    try {
      return new OldNamespaceTable(dContext.getStorage().getPlugin(schemaPath.get(0)), schemaConfig, datasetConfig);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
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
        try {
          return dContext.getStorage().getPlugin(schemaPath.get(0)).getFunctions(getChildPath(name), schemaConfig);
        } catch (ExecutionSetupException e) {
          throw new RuntimeException(e);
        }
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
    try {
      final StoragePlugin storagePlugin = dContext.getStorage().getPlugin(schemaPath.get(0));
      return storagePlugin.createView(getChildPath(view.getName()), view, schemaConfig);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
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
    try {
      dContext.getStorage().getPlugin(schemaPath.get(0)).dropView(schemaConfig, getChildPath(viewName));
      return;
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
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
    FileSystemPlugin fsPlugin;
    try {
      StoragePlugin storagePlugin = dContext.getStorage().getPlugin(schemaPath.get(0));
      if (storagePlugin instanceof FileSystemPlugin) {
        fsPlugin = (FileSystemPlugin) storagePlugin;
      } else {
        throw new UnsupportedOperationException("CTAS not supported for " + storagePlugin.getClass().getName());
      }
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }

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
      final FormatPluginConfig formatConfig =
          fsPlugin.createConfigForTable(tableName, storageOptions);
      formatPlugin = fsPlugin.getFormatPlugin(formatConfig);
    }

    String userName = fsPlugin.getConfig().isImpersonationEnabled() ? schemaConfig.getUserName() : ImpersonationUtil.getProcessUserName();
    return new FileSystemCreateTableEntry(
        userName,
        fsPlugin,
        formatPlugin,
        new Path(fsPlugin.getConfig().getPath(), removeLeadingSlash(tableName)).toString(),
        options);
  }

  public FileSystemWrapper getFileSystem() {
    StoragePlugin plugin = null;
    try {
      plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
    FileSystemPlugin fsPlugin;
    if (plugin instanceof FileSystemPlugin) {
      fsPlugin = (FileSystemPlugin) plugin;
    } else {
      throw new UnsupportedOperationException(plugin.getClass().getName() + " does not support FileSystem operations");
    }
    return fsPlugin.getFS(schemaConfig.getUserName());
  }

  public String getDefaultLocation() {
    StoragePlugin plugin = null;
    try {
      plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
    FileSystemPlugin fsPlugin;
    if (plugin instanceof FileSystemPlugin) {
      fsPlugin = (FileSystemPlugin) plugin;
    } else {
      throw new UnsupportedOperationException(plugin.getClass().getName() + " does not support FileSystem operations");
    }
    return fsPlugin.getConfig().getPath();
  }

  @Override
  public void dropTable(String tableName) {
    try {
      StoragePlugin plugin = dContext.getStorage().getPlugin(schemaPath.get(0));
      FileSystemPlugin fsPlugin;
      if (plugin instanceof FileSystemPlugin) {
        fsPlugin = (FileSystemPlugin) plugin;
      } else {
        throw new UnsupportedOperationException(plugin.getClass().getName() + " does not support dropping tables");
      }
      fsPlugin.dropTable(getChildPath(tableName), schemaConfig);
    } catch (ExecutionSetupException e) {
      throw new RuntimeException(e);
    }
  }

  private static DatasetConfig getDataset(NamespaceService ns, NamespaceKey key) {
    try {
      return ns.getDataset(key);
    } catch (NamespaceNotFoundException e) {
      return null;
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  private List<String> getChildPath(final String childName) {
    return ImmutableList.<String>builder().addAll(schemaPath).add(childName).build();
  }

  @Override
  public String getTypeName() {
    return "simple";
  }
}
