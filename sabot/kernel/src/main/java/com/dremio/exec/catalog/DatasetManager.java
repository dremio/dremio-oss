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

import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.datastore.IndexedStore.FindByCondition;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.exec.catalog.ManagedStoragePlugin.MetadataAccessType;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.Views;
import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIndexKeys;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceUtils;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * The workhorse of Catalog, responsible for retrieving datasets and interacting with sources as
 * necessary to complete a query.
 *
 * This operates entirely in the context of the user's namespace with the exception of the
 * DatasetSaver, which hides access to the SystemUser namespace solely for purposes of retrieving
 * datasets.
 */
class DatasetManager {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DatasetManager.class);

  private final PluginRetriever plugins;
  private final NamespaceService userNamespaceService;

  public DatasetManager(
      PluginRetriever plugins,
      NamespaceService userNamespaceService
      ) {
    this.userNamespaceService = userNamespaceService;
    this.plugins = plugins;
  }

  /**
   * A path is ambiguous if it has two parts and the root contains a period. In this case, we don't
   * know whether the root should be considered a single part of many parts and need to do a search
   * for an unescaped path rather than a lookup.
   *
   * This is because JDBC & ODBC tools use a two part naming scheme and thus we also present Dremio
   * datasets using this two part scheme where all parts of the path except the leaf are presented
   * as part of the schema of the table. This relates to DX-
   *
   * @param key
   *          Key to test
   * @return Whether path is ambiguous.
   */
  private boolean isAmbiguousKey(NamespaceKey key) {
    if(key.size() != 2) {
      return false;
    }

    return key.getRoot().contains(".");
  }

  private DatasetConfig getConfig(final NamespaceKey key) {
    if(!isAmbiguousKey(key)) {
      try {
        return userNamespaceService.getDataset(key);
      } catch(NamespaceNotFoundException ex) {
        return null;
      } catch(NamespaceException ex) {
        throw Throwables.propagate(ex);
      }
    }

    /**
     * If we have an ambgious key, let's search for possible matches in the namespace.
     *
     * From there we will:
     * - only consider keys that have the same leaf value (since ambiguity isn't allowed there)
     * - return the first key by ordering the keys by segment (cis then cs).
     */
    final FindByCondition condition = new FindByCondition();
    condition.setCondition(SearchQueryUtils.newTermQuery(NamespaceIndexKeys.UNQUOTED_LC_PATH, key.toUnescapedString().toLowerCase()));
    List<DatasetConfig> possibleMatches = FluentIterable.from(userNamespaceService.find(condition)).filter(new Predicate<Entry<NamespaceKey, NameSpaceContainer>>() {
      @Override
      public boolean apply(Entry<NamespaceKey, NameSpaceContainer> entry) {
        return entry.getKey().getLeaf().equalsIgnoreCase(key.getLeaf());
      }}).transform(new Function<Entry<NamespaceKey, NameSpaceContainer>, DatasetConfig>(){
        @Override
        public DatasetConfig apply(Entry<NamespaceKey, NameSpaceContainer> input) {
          return input.getValue().getDataset();
        }}).toSortedList(new Comparator<DatasetConfig>(){
          @Override
          public int compare(DatasetConfig o1, DatasetConfig o2) {
            List<String> p1 = o1.getFullPathList();
            List<String> p2 = o2.getFullPathList();

            final int size = Math.max(p1.size(), p2.size());

            for(int i = 0; i < size; i++) {

              // do a case insensitive order first.
              int cmp = p1.get(i).toLowerCase().compareTo(p2.get(i).toLowerCase());
              if(cmp != 0) {
                return cmp;
              }

              // do a case sensitive order second.
              cmp = p1.get(i).compareTo(p2.get(i));
              if(cmp != 0) {
                return cmp;
              }

            }

            Preconditions.checkArgument(p1.size() == p2.size(), "Two keys were indexed the same but had different lengths. %s, %s", p1, p2);
            return 0;
          }});

    if(possibleMatches.isEmpty()) {
      return null;
    }

    return possibleMatches.get(0);
  }

  private DatasetConfig getConfig(final String datasetId) {
    return userNamespaceService.findDatasetByUUID(datasetId);
  }

  public DremioTable getTable(
      NamespaceKey key,
      MetadataRequestOptions options
      ){

    final ManagedStoragePlugin plugin;

    final DatasetConfig config = getConfig(key);
    if(config != null) {
      // canonicalize the path.
      key = new NamespaceKey(config.getFullPathList());
    }

    if(key.getRoot().startsWith("@")) {
      plugin = plugins.getPlugin("__home", true);
    } else {
      plugin = plugins.getPlugin(key.getRoot(), false);
    }

    if(plugin != null) {

      // if we have a plugin and the info isn't a vds (this happens in home, where VDS are intermingled with plugin datasets).
      if(config == null || config.getType() != DatasetType.VIRTUAL_DATASET) {
        return getTableFromPlugin(key, config, plugin, options);
      }
    }

    if(config == null) {
      return null;
    }

    // at this point, we should only be looking at virtual datasets.
    if(config.getType() != DatasetType.VIRTUAL_DATASET) {
      // if we're not looking at a virtual dataset, it must mean that we hit a race condition where the source has been removed but the dataset was retrieved just before.
      return null;
    }

    return createTableFromVirtualDataset(config, options);
  }

  public DremioTable getTable(
    String datasetId,
    MetadataRequestOptions options
  ) {
    final DatasetConfig config = getConfig(datasetId);

    if (config == null) {
      return null;
    }

    NamespaceKey key = new NamespaceKey(config.getFullPathList());

    return getTable(key, options);
  }

  /**
   * Retrieves a source table, checking that things are up to date.
   * @param key
   * @param datasetConfig
   * @param plugin
   * @param options
   * @return
   */
  private DremioTable getTableFromPlugin(NamespaceKey key, DatasetConfig datasetConfig, ManagedStoragePlugin plugin, final MetadataRequestOptions options) {

    final Stopwatch stopwatch = Stopwatch.createStarted();
    final int maxMetadataColumns = plugin.getMaxMetadataColumns().get();
    if(plugin.isValid(datasetConfig, options)) {
      plugin.checkAccess(key, datasetConfig, options);
      final NamespaceKey canonicalKey = new NamespaceKey(datasetConfig.getFullPathList());
      final NamespaceTable namespaceTable = new NamespaceTable(new TableMetadataImpl(plugin.getId(), datasetConfig, options.getSchemaConfig().getUserName(), DatasetSplitsPointer.of(userNamespaceService, datasetConfig)));
      options.getStatsCollector().addDatasetStat(canonicalKey.getSchemaPath(), MetadataAccessType.CACHED_METADATA.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
      return namespaceTable;
    }

    try {

      // TODO: move views to namespace and out of filesystem.
      if(datasetConfig == null) {
        ViewTable view = plugin.getView(key, options);
        if(view != null){
          return view;
        }
      }
    } catch (UserException ex) {
      throw ex;
    } catch (Exception ex) {
      logger.warn("Exception while trying to read view.", ex);
      return null;
    }


    if(datasetConfig != null) {
      // canonicalize key if we can.
      key = new NamespaceKey(datasetConfig.getFullPathList());
    }

    // even though the following call won't assume the key is canonical we will need to hit the source anyway
    // to retrieve the complete dataset
    final SourceTableDefinition tableDefinition;

    try {
      tableDefinition = plugin.getTable(key, datasetConfig, options.getSchemaConfig().getIgnoreAuthErrors());
    } catch (Exception ex) {
      throw UserException.validationError(ex).message("Failure while retrieving dataset [%s].", key).build(logger);
    }

    if(tableDefinition == null) {
      return null;
    }

    final NamespaceKey canonicalKey = tableDefinition.getName();

    if(datasetConfig == null && !canonicalKey.equals(key)) {
      // before we do anything with this accessor, we should reprobe namespace as it is possible that the request the user made was not the canonical key and therefore we missed when trying to retrieve data from the namespace.

      try {
        datasetConfig = userNamespaceService.getDataset(canonicalKey);
        if(datasetConfig != null && plugin.isValid(datasetConfig, options)) {
          // if the datasetconfig is complete and unexpired, we'll recurse because we don't need the just retrieved SourceTableDefinition. Since they're lazy, little harm done.
          // Otherwise, we'll fall through and use the found accessor.
          return getTableFromPlugin(canonicalKey, datasetConfig, plugin, options);
        }
      } catch(NamespaceException e) {
        // ignore, we'll fall through.
      }
    }


    // arriving here means that the metadata for the table is either incomplete, missing or out of date. We need to save it and return the updated data.
    try {
      final DatasetConfig newDatasetConfig = tableDefinition.getDataset();
      final List<DatasetSplit> splits = tableDefinition.getSplits();
      NamespaceUtils.copyFromOldConfig(datasetConfig, newDatasetConfig);
      if (BatchSchema.fromDataset(newDatasetConfig).getFieldCount() > maxMetadataColumns) {
        throw UserException.validationError()
            .message(String.format("Using datasets with more than %d columns is currently disabled.",
                maxMetadataColumns))
            .build(logger);
      }

      // if saveable, we'll save whether or not
      if(tableDefinition.isSaveable()) {
        plugin.getSaver().completeSave(newDatasetConfig, splits);
      }

      options.getStatsCollector().addDatasetStat(canonicalKey.getSchemaPath(), MetadataAccessType.PARTIAL_METADATA.name(), stopwatch.elapsed(TimeUnit.MILLISECONDS));

      plugin.checkAccess(canonicalKey, newDatasetConfig, options);

      TableMetadata metadata = new TableMetadataImpl(plugin.getId(), newDatasetConfig, options.getSchemaConfig().getUserName(), MaterializedSplitsPointer.of(splits, splits.size()));
      return new NamespaceTable(metadata);
    } catch (DatasetMetadataTooLargeException e) {
      throw UserException.validationError(e)
        .message(String.format("Using datasets with more than %d columns is currently disabled.", maxMetadataColumns))
        .build(logger);
    } catch(UserException ex) {
      throw ex;
    } catch (Exception e) {
      throw UserException.dataReadError(e).message("Failure while attempting to read metadata for %s.", key).build(logger);
    }
  }

  private ViewTable createTableFromVirtualDataset(DatasetConfig datasetConfig, MetadataRequestOptions options) {
    try {
      View view = Views.fieldTypesToView(
          Iterables.getLast(datasetConfig.getFullPathList()),
          datasetConfig.getVirtualDataset().getSql(),
          ViewFieldsHelper.getCalciteViewFields(datasetConfig),
          datasetConfig.getVirtualDataset().getContextList()
      );

      // 1.4.0 and earlier didn't correctly save virtual dataset schema information.
      BatchSchema schema = DatasetHelper.getSchemaBytes(datasetConfig) != null ? BatchSchema.fromDataset(datasetConfig) : null;
      return new ViewTable(new NamespaceKey(datasetConfig.getFullPathList()), view, datasetConfig, schema);
    } catch (Exception e) {
      logger.warn("Failure parsing virtual dataset, not including in available schema.", e);
      return null;
    }
  }

  private boolean isFSBasedDataset(DatasetConfig datasetConfig) {
    return datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER ||
      datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER;
  }

  public boolean createOrUpdateDataset(NamespaceService userNamespaceService, ManagedStoragePlugin plugin, NamespaceKey source, final NamespaceKey datasetPath, final DatasetConfig datasetConfig, NamespaceAttribute... attributes) throws NamespaceException {
    final int maxLeafColumns = plugin.getMaxMetadataColumns().get();
    if (!isFSBasedDataset(datasetConfig)) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig, attributes);
    }
    DatasetConfig oldDatasetConfig = null;
    try {
      oldDatasetConfig = userNamespaceService.getDataset(datasetPath);
    } catch (NamespaceNotFoundException nfe) {
      // ignore
    }

    // if format settings did not change fall back to namespace based update
    if (oldDatasetConfig != null && oldDatasetConfig.getPhysicalDataset().getFormatSettings().equals(datasetConfig.getPhysicalDataset().getFormatSettings())) {
      return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig, attributes);
    }

    try {
      // for home files dataset path is location of file not path in namespace.
      if (datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FILE || datasetConfig.getType() == DatasetType.PHYSICAL_DATASET_HOME_FOLDER) {
        final SourceTableDefinition datasetAccessor = plugin.getTable(
          new NamespaceKey(ImmutableList.<String>builder()
            .add("__home") // TODO (AH) hack.
            .addAll(PathUtils.toPathComponents(datasetConfig.getPhysicalDataset().getFormatSettings().getLocation())).build()),
          datasetConfig, false);
        if (datasetAccessor == null) {
          return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig, attributes);
        }
        saveInHomeSpace(userNamespaceService, datasetAccessor, datasetConfig, maxLeafColumns);
      } else {
        final SourceTableDefinition datasetAccessor = plugin.getTable(datasetPath, datasetConfig, false);
        if (datasetAccessor == null) {
          // setting format setting on empty folders?
          return userNamespaceService.tryCreatePhysicalDataset(datasetPath, datasetConfig, attributes);
        }

        try {
          plugin.getSaver()
            .datasetSave(datasetAccessor, oldDatasetConfig, maxLeafColumns, attributes);
        } catch (DatasetMetadataTooLargeException e) {
          throw UserException.validationError()
            .message(String.format("Using datasets with more than %d columns is currently disabled.", maxLeafColumns))
            .build(logger);
        }
      }
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, NamespaceException.class);
      throw new RuntimeException("Failed to get new dataset ", e);
    }
    return true;
  }

  public void createDataset(NamespaceKey key, ManagedStoragePlugin plugin, Function<DatasetConfig, DatasetConfig> datasetMutator) {
    final int maxLeafColumns = plugin.getMaxMetadataColumns().get();
    DatasetConfig config = null;
    try {
      config = userNamespaceService.getDataset(key);
      if(config != null){
        throw UserException.validationError().message("Table already exists %s", key.getRoot()).build(logger);
      }
    }catch (NamespaceException ex){
      logger.debug("Failure while trying to retrieve dataset for key {}.", key, ex);
    }

    SourceTableDefinition definition = null;
    try {
      definition = plugin.getTable(key, null, false);
    } catch (Exception ex){
      throw UserException.dataReadError(ex).message("Failure while attempting to read metadata for table %s from source.", key).build(logger);
    }

    if(definition == null){
      throw UserException.validationError().message("Unable to find requested table %s.", key).build(logger);
    }

    try {
      plugin.getSaver()
          .datasetSave(datasetMutator == null ? definition : new MutatedSourceTableDefinition(definition, datasetMutator),
            config, maxLeafColumns);
    } catch (DatasetMetadataTooLargeException e) {
      throw UserException.validationError(e)
          .message(String.format("Using datasets with more than %d columns is currently disabled.",
              maxLeafColumns))
          .build(logger);
    }
  }

  private void saveInHomeSpace(NamespaceService namespaceService, SourceTableDefinition accessor, DatasetConfig nsConfig, int maxMetadataLeafColumns) {
    Preconditions.checkNotNull(nsConfig);
    final NamespaceKey key = new NamespaceKey(nsConfig.getFullPathList());
    try{
      // use key from namespace config
      DatasetConfig srcConfig = accessor.getDataset();
      if (nsConfig.getId() == null) {
        nsConfig.setId(srcConfig.getId());
      }
      // Merge namespace config with config obtained from underlying filesystem used to store user uploaded files.
      // Set schema, read definition and state from source accessor
      nsConfig.setRecordSchema(srcConfig.getRecordSchema());
      nsConfig.setSchemaVersion(srcConfig.getSchemaVersion());
      nsConfig.setReadDefinition(srcConfig.getReadDefinition());
      // get splits from source
      List<DatasetSplit> splits = accessor.getSplits();
      namespaceService.addOrUpdateDataset(key, nsConfig, splits);
    } catch (DatasetMetadataTooLargeException e) {
      throw UserException.validationError(e)
        .message(String.format("Using datasets with more than %d columns is currently disabled.", maxMetadataLeafColumns))
        .build(logger);
    } catch(Exception ex){
      logger.warn("Failure while retrieving and saving dataset {}.", key, ex);
    }
  }

}
