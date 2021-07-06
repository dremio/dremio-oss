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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.util.Preconditions;
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
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.MaterializedSplitsPointer;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.HashPrelUtil;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.RoundRobinExchangePrel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.WriterCommitterPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.NamespaceTable;
import com.dremio.exec.store.PartitionChunkListingImpl;
import com.dremio.exec.store.SplitsPointer;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;
import com.dremio.exec.store.metadatarefresh.dirlisting.DirListingScanPrel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.NamespaceException;
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
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.namespace.file.proto.FileType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.ImmutableList;

/**
 * Helps in building RefreshDataset plans. Default implementation creates a full refresh plan for a new filesystem dataset.
 * Abstractions can be done extending this unit for different branches incremental and partial refresh use-cases.
 */
public class RefreshDatasetPlanBuilder {
    public static final String METADATA_STORAGEPLUGIN_NAME = "__metadata";

    // nothing is known about the dataset yet. Imagining high value at start for max parallelism.
    private static final long INITIAL_RECORD_COUNT = 1_000_000L;

    protected static final Logger logger = LoggerFactory.getLogger(RefreshDatasetPlanBuilder.class);
    protected final SqlHandlerConfig config;
    protected final RelOptCluster cluster;
    protected final RelTraitSet traitSet;
    protected final SupportsInternalIcebergTable plugin;
    protected final RelOptTable table;
    protected final StoragePluginId storagePluginId;
    protected final RefreshExecTableMetadata refreshExecTableMetadata;
    protected final String userName;
    protected final NamespaceKey tableNSKey;
    protected final FileSystemPlugin<?> metaStoragePlugin;
    protected MetadataProvider metadataProvider;

    public RefreshDatasetPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset) throws IOException, NamespaceException {
        this.config = config;
        cluster = config.getConverter().getCluster();
        traitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL);
        final Catalog catalog = config.getContext().getCatalog();
        tableNSKey = catalog.resolveSingle(new NamespaceKey(sqlRefreshDataset.getTable().names));
        logger.debug("Setting up refresh plan for {}", tableNSKey);
        plugin = getPlugin(catalog, tableNSKey);
        userName = config.getContext().getQueryUserName();
        storagePluginId = config.getContext().getCatalogService().getManagedSource(tableNSKey.getRoot()).getId();
        final DremioCatalogReader catalogReader = config.getConverter().getCatalogReader();
        metaStoragePlugin = config.getContext().getCatalogService().getSource(METADATA_STORAGEPLUGIN_NAME);
        final DatasetConfig datasetConfig = setupDatasetConfig();
        final SplitsPointer splits = getRootFolderSplits(tableNSKey, plugin, config, sqlRefreshDataset, datasetConfig);
        refreshExecTableMetadata = new RefreshExecTableMetadata(storagePluginId, datasetConfig, userName, splits, metadataProvider.getTableSchema());
        final NamespaceTable nsTable = new NamespaceTable(refreshExecTableMetadata, true);
        this.table = new DremioPrepareTable(catalogReader, JavaTypeFactoryImpl.INSTANCE, nsTable);
    }

    private DatasetConfig setupDatasetConfig() {
        // Assuming parquet as the data type. Use detected file type after DX-33404 is fixed
        final FileConfig format = new FileConfig();
        format.setType(FileType.PARQUET);
        format.setLocation(tableNSKey.toString());
        final PhysicalDataset physicalDataset = new PhysicalDataset();
        physicalDataset.setFormatSettings(format);
        final ReadDefinition initialReadDef = ReadDefinition.getDefaultInstance();
        final ScanStats stats = new ScanStats();
        stats.setType(ScanStatsType.NO_EXACT_ROW_COUNT);
        stats.setScanFactor(ScanCostFactor.PARQUET.getFactor());
        stats.setRecordCount(INITIAL_RECORD_COUNT);
        initialReadDef.setScanStats(stats);

        DatasetConfig datasetConfig = DatasetConfig.getDefaultInstance();
        datasetConfig.setFullPathList(tableNSKey.getPathComponents());
        datasetConfig.setPhysicalDataset(physicalDataset);
        datasetConfig.setId(new EntityId(UUID.randomUUID().toString()));
        datasetConfig.setReadDefinition(initialReadDef);
        return datasetConfig;
    }

    /**
     * Outputs root Prel
     *
     * @return
     * @throws IOException
     */
    public Prel buildPlan() throws RelConversionException {
        final Prel dirListingPrel = getDataFileListingPrel();
        final Prel roundRobinExchange = getRoundRobinExchange(dirListingPrel);
        final Prel footerReadPrel = getFooterReader(roundRobinExchange);
        final Prel hashToRandomExchange = getHashToRandomExchange(footerReadPrel);
        final Prel writerCommitterPrel = getWriterPrel(hashToRandomExchange);
        final Prel screenPrel = new ScreenPrel(cluster, traitSet, writerCommitterPrel);

        return screenPrel;
    }

    // Extended to calculate diff for incremental use-cases
    protected Prel getDataFileListingPrel() throws RelConversionException {
      Prel dirListingScanPrel =  new DirListingScanPrel(cluster, traitSet, table, storagePluginId, refreshExecTableMetadata, getRowCountAdjustment());

      RexBuilder rexBuilder = cluster.getRexBuilder();

      RexNode isRemoved = rexBuilder.makeLiteral(false);

      List<RexNode> projectExpressions = new ArrayList<>();
      List<String> projectFields = new ArrayList<>();

      projectExpressions.add(isRemoved);
      projectFields.add(MetadataRefreshExecConstants.isDeletedFile);

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
        final TableFunctionConfig readerFooterConfig = TableFunctionUtil.getFooterReadFunctionConfig(refreshExecTableMetadata, schemaIfKnown, null);
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
                ImmutableList.copyOf(cols),
                readerFooterConfig,
                footerReadTFRowType,
                TableFunctionPrel.TableFunctionPOPCreator.DEFAULT);
    }

    protected Prel getWriterPrel(Prel childPrel) {
      IcebergTableProps icebergTableProps = getWriterConfig();

      final WriterOptions writerOptions = new WriterOptions(null, null, null, null,
                null, false, Long.MAX_VALUE, null, null, icebergTableProps);
      final FileSystemCreateTableEntry fsCreateTableEntry = new FileSystemCreateTableEntry(userName, metaStoragePlugin,
                metaStoragePlugin.getFormatPlugin("iceberg"), icebergTableProps.getTableLocation(), icebergTableProps, writerOptions);
      final RelTraitSet singletonTraitSet = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);
      final WriterPrel writerPrel = new WriterPrel(cluster, singletonTraitSet, childPrel, fsCreateTableEntry, childPrel.getRowType());
      return new WriterCommitterPrel(cluster, singletonTraitSet, writerPrel, metaStoragePlugin,
        null, icebergTableProps.getTableLocation(), userName, fsCreateTableEntry);
    }

    protected IcebergTableProps getWriterConfig() {
      final String tableMappingPath = Path.of(metaStoragePlugin.getConfig().getPath().toString()).resolve(metadataProvider.getTableUUId()).toString();
      // If dist is local, ensure it is prefixed with the scheme in the dremio.conf - `file:///`
      logger.info("Writing metadata for {} at {}", tableNSKey, tableMappingPath);

      final IcebergTableProps icebergTableProps = new IcebergTableProps(tableMappingPath, metadataProvider.getTableUUId(),
        metadataProvider.getTableSchema(), metadataProvider.getPartitionColumn(), IcebergCommandType.FULL_METADATA_REFRESH, tableNSKey.getSchemaPath());
      icebergTableProps.setDetectSchema(!plugin.canGetDatasetMetadataInCoordinator());
      icebergTableProps.setMetadataRefresh(true);

      return icebergTableProps;
    }

    private Prel getRoundRobinExchange(Prel child) {
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
        false /* Since we want a project to hash the expression */);
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

    private SplitsPointer getRootFolderSplits(NamespaceKey tableNSKey, SupportsInternalIcebergTable plugin, SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset, DatasetConfig datasetConfig) throws IOException {
        PartitionChunkListing chunkListing;
        DatasetRetrievalOptions options = DatasetRetrievalOptions.DEFAULT.toBuilder().setRefreshDataset(true).build();
        Optional<DatasetHandle> handle = Optional.empty();

        // For plugins like Hive, Aws glue we get dataset metadata like partition spec, partition values, table schema
        // in the coordinator, whereas for FileSystemPlugins metadata is built in the executors.
        if (plugin.canGetDatasetMetadataInCoordinator()) {
          SourceMetadata sourceMetadata = (SourceMetadata) plugin;
          EntityPath entityPath = MetadataObjectsUtils.toEntityPath(tableNSKey);
          handle = sourceMetadata.getDatasetHandle((entityPath), options.asGetDatasetOptions(datasetConfig));
          if (!handle.isPresent()) { // dataset is not in the source
            throw new DatasetNotFoundException(entityPath);
          }
          else {
            chunkListing = sourceMetadata.listPartitionChunks(handle.get(), options.asListPartitionChunkOptions(datasetConfig));
          }
        }
        else {
          final Path path = FileSelection.getPathBasedOnFullPath(plugin.resolveTableNameToValidPath(tableNSKey.getPathComponents()));
          logger.debug("Dataset's full path is {}", path);

          final DirListInputSplitProto.DirListInputSplit dirListInputSplit = DirListInputSplitProto.DirListInputSplit.newBuilder()
            .setPath(path.toString())
            .setReadSignature(Long.MAX_VALUE)
            .build();

          final DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 0, 1, dirListInputSplit::writeTo);
          final PartitionChunkListingImpl partitionChunkListing = new PartitionChunkListingImpl();
          partitionChunkListing.put(Collections.emptyList(), split);
          partitionChunkListing.computePartitionChunks();
          chunkListing = partitionChunkListing;
        }

        // Convert to partitionChunkMetadataList
        List<PartitionChunkMetadata> partitionChunkMetadataList = new ArrayList<>();
        final Iterator<? extends com.dremio.connector.metadata.PartitionChunk> chunks = chunkListing.iterator();
        int splitCount = 0;
        while (chunks.hasNext()) {
          final com.dremio.connector.metadata.PartitionChunk chunk = chunks.next();
          PartitionProtobuf.PartitionChunk partitionChunkProto = convertToPartitionChunkProto(chunk);
          final PartitionChunkId splitId = PartitionChunkId.of(datasetConfig, partitionChunkProto, 1L);
          partitionChunkMetadataList.add(new PartitionChunkMetadataImpl(partitionChunkProto, splitId));
          splitCount++;
        }

        if (plugin.canGetDatasetMetadataInCoordinator()) {
          final DatasetMetadata datasetMetadata = ((SourceMetadata) plugin).getDatasetMetadata(handle.get(), chunkListing,
            options.asGetMetadataOptions(datasetConfig));
          metadataProvider = new HiveMetadataProvider(config, sqlRefreshDataset, datasetMetadata);
        }
        else {
          metadataProvider = new MetadataProvider(config, sqlRefreshDataset);
        }

        return MaterializedSplitsPointer.of(0, partitionChunkMetadataList, splitCount);
    }

    protected static RelDataType getRowTypeFromProjectedCols(BatchSchema outputSchema, List<SchemaPath> projectedColumns, RelOptCluster cluster) {
        LinkedHashSet<String> firstLevelPaths = new LinkedHashSet<>();
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

    private static PartitionProtobuf.PartitionChunk convertToPartitionChunkProto(PartitionChunk chunk) {
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

    private boolean isFirstQuery() {
        return true;
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
}
