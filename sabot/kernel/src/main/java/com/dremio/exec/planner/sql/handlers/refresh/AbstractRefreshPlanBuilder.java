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
package com.dremio.exec.planner.sql.handlers.refresh;

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.RoundRobinExchangePrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.RepairKvstoreFromIcebergMetadata;
import com.dremio.exec.store.iceberg.IcebergManifestWriterPrel;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.PartitionChunkMetadataImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.dirlist.proto.DirListInputSplitProto;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Base plan builder class. Should be extended by all other plan builders.
 */
public abstract class AbstractRefreshPlanBuilder implements MetadataRefreshPlanBuilder {

    // nothing is known about the dataset yet. Imagining high value at start for max parallelism.
    protected static final long INITIAL_RECORD_COUNT = DremioCost.LARGE_ROW_COUNT;
    protected static final Logger logger = LoggerFactory.getLogger(AbstractRefreshPlanBuilder.class);
    protected final SqlHandlerConfig config;
    protected final RelOptCluster cluster;
    protected final RelTraitSet traitSet;
    protected final SupportsInternalIcebergTable plugin;
    protected RelOptTable table;
    protected final StoragePluginId storagePluginId;
    protected RefreshExecTableMetadata refreshExecTableMetadata;
    protected final String userName;
    protected final FileSystemPlugin<?> metaStoragePlugin;
    protected final UnlimitedSplitsMetadataProvider metadataProvider;
    protected DatasetConfig datasetConfig;
    protected boolean isFileDataset = false;

    protected NamespaceKey tableNSKey;
    protected IcebergCommandType icebergCommandType;
    protected BatchSchema tableSchema;
    protected List<String> partitionCols;
    protected SqlRefreshDataset sqlNode;
    protected Path datasetPath;
    protected Catalog catalog;
    protected FileType datasetFileType;
    protected boolean readSignatureEnabled;
    protected String tableRootPath;

    public AbstractRefreshPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, UnlimitedSplitsMetadataProvider metadataProvider) {
        this.config = config;
        this.metadataProvider = metadataProvider;
        this.sqlNode = sqlRefreshDataset;
        cluster = config.getConverter().getCluster();
        traitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL);
        catalog = config.getContext().getCatalog();
        tableNSKey = catalog.resolveSingle(new NamespaceKey(sqlRefreshDataset.getTable().names));
        logger.debug("Setting up refresh plan for {}", tableNSKey);
        plugin = getPlugin(catalog, tableNSKey);
        userName = config.getContext().getQueryUserName();
        storagePluginId = config.getContext().getCatalogService().getManagedSource(tableNSKey.getRoot()).getId();
        metaStoragePlugin = config.getContext().getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);
        datasetPath = FileSelection.getPathBasedOnFullPath(plugin.resolveTableNameToValidPath(tableNSKey.getPathComponents()));
        icebergCommandType = IcebergCommandType.FULL_METADATA_REFRESH;
        tableRootPath = datasetPath.toString();
        datasetConfig = setupDatasetConfig();
    }

    protected DatasetConfig setupDatasetConfig() {
        DatasetConfig datasetConf = metadataProvider.getDatasetConfig();
        if (datasetConf == null) {
          datasetConf = new DatasetConfig();
          datasetConf.setId(new EntityId(UUID.randomUUID().toString()));
        }

        final PhysicalDataset physicalDataset = new PhysicalDataset();
        final ReadDefinition initialReadDef = ReadDefinition.getDefaultInstance();
        final ScanStats stats = new ScanStats();
        stats.setType(ScanStatsType.NO_EXACT_ROW_COUNT);
        stats.setRecordCount(INITIAL_RECORD_COUNT);
        initialReadDef.setScanStats(stats);

        stats.setScanFactor(ScanCostFactor.EASY.getFactor());
        initialReadDef.setManifestScanStats(stats);

        datasetConf.setFullPathList(tableNSKey.getPathComponents());
        datasetConf.setPhysicalDataset(physicalDataset);
        datasetConf.setReadDefinition(initialReadDef);
        return datasetConf;
    }

  /**
   * returns true if datasetConfig is not in sync with iceberg metadata. In such cases datasetConfig is modified and saved.
   * returns false if datasetConfig is up-to-date with iceberg metadata
   * @return
   */
  public abstract boolean updateDatasetConfigWithIcebergMetadataIfNecessary();

  protected boolean repairAndSaveDatasetConfigIfNecessary() {
    RepairKvstoreFromIcebergMetadata repairOperation = new RepairKvstoreFromIcebergMetadata(datasetConfig, metaStoragePlugin,
      config.getContext().getNamespaceService(SystemUser.SYSTEM_USERNAME),
      catalog.getSource(tableNSKey.getRoot()),
      config.getContext().getOptions().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE));
    return repairOperation.checkAndRepairDatasetWithoutQueryRetry();
  }

  /**
   * Outputs root Prel
   *
   * @return
   */
  public Prel buildPlan() {
    final Prel dirListingPrel = getDataFileListingPrel();
    final Prel roundRobinExchange = getDirListToFooterReadExchange(dirListingPrel);
    final Prel footerReadPrel = getFooterReader(roundRobinExchange);
    final Prel hashToRandomExchange = getHashToRandomExchange(footerReadPrel);
    final Prel writerCommitterPrel = getWriterPrel(hashToRandomExchange);
    return new ScreenPrel(cluster, traitSet, writerCommitterPrel);
  }

  public abstract PartitionChunkListing listPartitionChunks(DatasetRetrievalOptions datasetRetrievalOptions) throws ConnectorException;

  public abstract void setupMetadataForPlanning(PartitionChunkListing partitionChunkListing, DatasetRetrievalOptions retrievalOptions) throws ConnectorException, InvalidProtocolBufferException;

  // Extended to calculate diff for incremental use-cases
  public Prel getDataFileListingPrel() {
      Prel dirListingScanPrel =  new DirListingScanPrel(
        cluster, traitSet, table, storagePluginId, refreshExecTableMetadata, getRowCountAdjustment(),
        true, x -> getRowCountEstimates("DirList"), ImmutableList.of());

      RexBuilder rexBuilder = cluster.getRexBuilder();
      RexNode isRemoved = rexBuilder.makeLiteral(false);

      List<RexNode> projectExpressions = new ArrayList<>();
      List<String> projectFields = new ArrayList<>();

      projectExpressions.add(isRemoved);
      projectFields.add(MetadataRefreshExecConstants.IS_DELETED_FILE);

      List<Field> dirListInputFields = MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields();

      dirListInputFields.forEach(x -> {
        projectFields.add(x.getName());
        Pair<Integer, RelDataTypeField> tempRef = findFieldWithIndex(dirListingScanPrel, x.getName());
        projectExpressions.add(rexBuilder.makeInputRef(tempRef.right.getType(), tempRef.left));
      });

      RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);
      return ProjectPrel.create(dirListingScanPrel.getCluster(), dirListingScanPrel.getTraitSet(), dirListingScanPrel, projectExpressions, newRowType);
    }

    protected Prel getFooterReader(Prel child) {
        final BatchSchema schemaIfKnown = plugin.canGetDatasetMetadataInCoordinator() ? refreshExecTableMetadata.getTableSchema() : null;
        final TableFunctionConfig readerFooterConfig = TableFunctionUtil.getFooterReadFunctionConfig(refreshExecTableMetadata, schemaIfKnown, null, datasetFileType);
        final List<SchemaPath> cols = MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields()
                .stream()
                .map(field -> SchemaPath.getSimplePath(field.getName()))
                .collect(Collectors.toList());
        final RelDataType footerReadTFRowType = getRowTypeFromProjectedCols(MetadataRefreshExecConstants.FooterRead.OUTPUT_SCHEMA.BATCH_SCHEMA, cols, cluster);
        return new TableFunctionPrel(
                cluster,
                traitSet,
                table,
                child,
                refreshExecTableMetadata,
                readerFooterConfig,
                footerReadTFRowType,
                x -> getRowCountEstimates("FooterReadTableFunction"));
    }

    protected Prel getWriterPrel(Prel childPrel) {
      IcebergTableProps icebergTableProps = getIcebergTableProps();
      final WriterOptions writerOptions = new WriterOptions(null, null, null, null,
          null, false, Long.MAX_VALUE, null, null, icebergTableProps, readSignatureEnabled);
      final CreateTableEntry fsCreateTableEntry = new CreateParquetTableEntry(userName, metaStoragePlugin,
        icebergTableProps.getTableLocation(), icebergTableProps, writerOptions, tableNSKey, storagePluginId);

      final DistributionTrait childDist = childPrel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
      final RelTraitSet childTraitSet = traitSet.plus(childDist).plus(Prel.PHYSICAL);

      final IcebergManifestWriterPrel writerPrel = new IcebergManifestWriterPrel(cluster, childTraitSet, childPrel, fsCreateTableEntry);

      final RelTraitSet singletonTraitSet = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);
      UnionExchangePrel exchangePrel = new UnionExchangePrel(cluster, singletonTraitSet, writerPrel);

      return new WriterCommitterPrel(cluster, singletonTraitSet, exchangePrel, metaStoragePlugin,
          null, icebergTableProps.getTableLocation(), userName, fsCreateTableEntry, Optional.of(datasetConfig), sqlNode.isPartialRefresh(), readSignatureEnabled, storagePluginId);
    }

    protected IcebergTableProps getIcebergTableProps() {
      final String tableMappingPath = Path.of(metaStoragePlugin.getConfig().getPath().toString()).resolve(metadataProvider.getTableUUId()).toString();
      // If dist is local, ensure it is prefixed with the scheme in the dremio.conf - `file:///`
      logger.info("Writing metadata for {} at {}", tableNSKey, tableMappingPath);

      List<String> partitionPaths = readSignatureEnabled ? getPartitionPaths(() -> refreshExecTableMetadata.getSplits()) : new ArrayList<>(); // This is later used in the executors only when computing read signature
      final IcebergTableProps icebergTableProps = new IcebergTableProps(tableMappingPath, metadataProvider.getTableUUId(),
        tableSchema, partitionCols, icebergCommandType, null,  tableNSKey.getSchemaPath(), tableRootPath, null, null, null);
      icebergTableProps.setPartitionPaths(partitionPaths);
      icebergTableProps.setDetectSchema(!plugin.canGetDatasetMetadataInCoordinator());
      icebergTableProps.setMetadataRefresh(true);
      icebergTableProps.setPersistedFullSchema(metadataProvider.getTableSchema());

      return icebergTableProps;
    }

    public Prel getDirListToFooterReadExchange(Prel child) {
      RelTraitSet relTraitSet = traitSet.plus(DistributionTrait.ROUND_ROBIN);
      return new RoundRobinExchangePrel(child.getCluster(), relTraitSet, child);
    }

    private Prel getHashToRandomExchange(Prel child) {
      DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
      DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
      RelTraitSet relTraitSet = traitSet.plus(distributionTrait);
      return new HashToRandomExchangePrel(
        cluster,
        relTraitSet,
        child,
        distributionTrait.getFields(),
        HashPrelUtil.DATA_FILE_DISTRIBUTE_HASH_FUNCTION_NAME,
        null /* Since we want a project to hash the expression */);
    }

    private SupportsInternalIcebergTable getPlugin(Catalog catalog, NamespaceKey key) {
        StoragePlugin plugin = catalog.getSource(key.getRoot());
        if (plugin instanceof SupportsInternalIcebergTable) {
            return (SupportsInternalIcebergTable) plugin;
        } else {
            throw UserException.validationError()
                    .message("Source identified was invalid type. REFRESH DATASET is only supported on sources that can contain files or folders")
                    .build(logger);
        }
    }

    protected static RelDataType getRowTypeFromProjectedCols(BatchSchema outputSchema, List<SchemaPath> projectedColumns, RelOptCluster cluster) {
        Set<String> firstLevelPaths = new LinkedHashSet<>();
        for(SchemaPath p : projectedColumns){
            firstLevelPaths.add(p.getRootSegment().getNameSegment().getPath());
        }
        final RelDataTypeFactory factory = cluster.getTypeFactory();
        final RelDataTypeFactory.FieldInfoBuilder builder = new RelDataTypeFactory.FieldInfoBuilder(factory);
        final Map<String, RelDataType> fields = new HashMap<>();
        final boolean isFullNestedSchemaSupport = PrelUtil.getPlannerSettings(cluster).isFullNestedSchemaSupport();
        for(Field field : outputSchema){
            if(firstLevelPaths.contains(field.getName())){
                fields.put(field.getName(), CalciteArrowHelper.wrap(CompleteType.fromField(field)).toCalciteType(factory,
                        isFullNestedSchemaSupport));
            }
        }

        Preconditions.checkArgument(firstLevelPaths.size() == fields.size(), "Projected column base size %s is not equal to outcome rowtype %s.", firstLevelPaths.size(), fields.size());

        for(String path : firstLevelPaths){
            builder.add(path, fields.get(path));
        }

        return builder.build();
    }

  public double getRowCountEstimates(String type) {
    if (isFileDataset) {
      return 1;
    }
    double rowCount = 0;

    switch (type) {
      case "DirList":
        rowCount = 1000_000L;
        break;
      case "FooterReadTableFunction":
        rowCount = DremioCost.LARGE_ROW_COUNT;
        break;
    }

    return Math.max(1, rowCount);
  }

  protected void checkAndUpdateIsFileDataset() {
  }

  protected double getRowCountAdjustment() {
    return 1.0;
  }

  private Pair<Integer, RelDataTypeField> findFieldWithIndex(RelNode relNode, String fieldName) {
    Pair<Integer, RelDataTypeField> fieldPair = MoreRelOptUtil.findFieldWithIndex(relNode.getRowType().getFieldList(), fieldName);

    if (fieldPair == null) {
      throw new RuntimeException(String.format("Unable to find field '%s' in the schema", fieldName));
    }
    return fieldPair;
  }

  public static List<PartitionChunkMetadata> convertToPartitionChunkMetadata(PartitionChunkListing chunkListing, DatasetConfig datasetConfig) {
    // Convert to partitionChunkMetadataList
    List<PartitionChunkMetadata> partitionChunkMetadataList = new ArrayList<>();
    final Iterator<? extends PartitionChunk> chunks = chunkListing.iterator();
    while (chunks.hasNext()) {
      final com.dremio.connector.metadata.PartitionChunk chunk = chunks.next();
      PartitionProtobuf.PartitionChunk partitionChunkProto = convertToPartitionChunkProto(chunk);
      final PartitionChunkId splitId = PartitionChunkId.of(datasetConfig, partitionChunkProto, 1L);
      partitionChunkMetadataList.add(new PartitionChunkMetadataImpl(partitionChunkProto, splitId));
    }
    return partitionChunkMetadataList;
  }

    static PartitionProtobuf.PartitionChunk convertToPartitionChunkProto(PartitionChunk chunk) {
    final List<PartitionProtobuf.PartitionValue> values = StreamSupport.stream(Spliterators.spliterator(chunk.getPartitionValues().iterator(),
      0, 0), false)
      .map(MetadataProtoUtils::toProtobuf)
      .collect(Collectors.toList());

    Iterator<? extends DatasetSplit> datasetSplitIterator = chunk.getSplits().iterator();
    int accumulatedRecordCount = 0, accumulatedSizeInBytes = 0, splitCount = 0;
    List<DatasetSplit> datasetSplits = new ArrayList<>();
    while (datasetSplitIterator.hasNext()) {
      DatasetSplit split = datasetSplitIterator.next();
      accumulatedRecordCount += split.getRecordCount();
      accumulatedSizeInBytes += split.getSizeInBytes();
      splitCount++;
      datasetSplits.add(split);
    }
    Preconditions.checkState(splitCount > 0, "No splits found");
    PartitionProtobuf.PartitionChunk.Builder builder = PartitionProtobuf.PartitionChunk.newBuilder()
      .setRowCount(accumulatedRecordCount)
      .setSize(accumulatedSizeInBytes)
      .addAllPartitionValues(values)
      .setSplitCount(splitCount);

    datasetSplits.forEach(d -> builder.setDatasetSplit(MetadataProtoUtils.toProtobuf(d)));
    builder.setSplitKey("metadata-split" + "-1");
    return builder.build();
  }

  public List<String> getAllPartitionPaths() {
    if (!sqlNode.isPartialRefresh()) { // refreshExecTableMetadata already has full list of partitions
      return getPartitionPaths(() -> refreshExecTableMetadata.getSplits());
    }

    // In partial refresh, the splits in refreshExecTableMetadata do not contain full listing from HMS.
    // Splits were filtered based on partitions specified in partial-refresh query. However, to restore read signature we need full listing
    DatasetRetrievalOptions retrievalOptions = retrievalOptionsForPartitions(config, Collections.emptyMap());
    PartitionChunkListing partitionChunkListing;
    try {
      partitionChunkListing = this.listPartitionChunks(retrievalOptions);
    } catch (ConnectorException e) {
      logger.error("Failure while listing partitions of table {}", datasetConfig.getFullPathList());
      throw UserException.ioExceptionError(e).message("Metadata of table %s is corrupted. Forget and re-promote the dataset.",
        datasetConfig.getFullPathList()).build(logger);
    }
    List<PartitionChunkMetadata> chunkMetadata = convertToPartitionChunkMetadata(partitionChunkListing, datasetConfig);
    return getPartitionPaths(chunkMetadata);
  }

  public List<String> getPartitionPaths(Iterable<PartitionChunkMetadata> splits) {
    Preconditions.checkNotNull(refreshExecTableMetadata);
    List<String> partitionPaths = new ArrayList<>();

    for (PartitionChunkMetadata split : splits) {
      // No. of dataset splits in PartitionChunkMetadata is expected to be 1
      for (PartitionProtobuf.DatasetSplit datasetSplit : split.getDatasetSplits()) {
        try {
          DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.parseFrom(datasetSplit.getSplitExtendedProperty().toByteArray());
          List<PartitionProtobuf.PartitionValue> partitionValues = Lists.newArrayList(split.getPartitionValues());
          if (!partitionValues.isEmpty()) {
            partitionPaths.add(dirListInputSplit.getOperatingPath());
          }
        }
        catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return partitionPaths;
  }

  public Prel getHashToRandomExchangePrel(Prel child) {
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = traitSet.plus(distributionTrait);
    return new HashToRandomExchangePrel(
      cluster,
      relTraitSet,
      child,
      distributionTrait.getFields(),
      HashPrelUtil.HASH32_FUNCTION_NAME,
      null /* Since we want a project to hash the expression */);
  }

  public static DatasetRetrievalOptions retrievalOptionsForPartitions(SqlHandlerConfig config, Map<String, String> partitions) {
    DatasetRetrievalOptions.Builder optionsBuilder = DatasetRetrievalOptions.DEFAULT.toBuilder().setRefreshDataset(true);
    if (partitions != null && !partitions.isEmpty()) {
      optionsBuilder.setPartition(partitions);
    }
    optionsBuilder.setMaxMetadataLeafColumns((int) config.getContext().getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
    optionsBuilder.setMaxNestedLevel((int) config.getContext().getOptions().getOption(CatalogOptions.MAX_NESTED_LEVELS));
    return optionsBuilder.build();
  }

  protected List<String> getPrimaryKey() {
    List<String> primaryKey = null;
    if (plugin instanceof MutablePlugin) {
      MutablePlugin mutablePlugin = (MutablePlugin) plugin;
      try {
        primaryKey = mutablePlugin.getPrimaryKey(tableNSKey, datasetConfig,
          SchemaConfig.newBuilder(CatalogUser.from(userName)).build(), null,
          !(mutablePlugin instanceof VersionedPlugin));
      } catch (Exception ex) {
        logger.debug("Failed to get primary key", ex);
      }
    }
    return primaryKey;
  }
}
