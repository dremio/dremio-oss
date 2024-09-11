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
package com.dremio.exec.store.iceberg;

import static com.dremio.exec.ExecConstants.ENABLE_READING_POSITIONAL_DELETE_WITH_ANTI_JOIN;
import static com.dremio.exec.ops.SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES;
import static com.dremio.exec.ops.SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS;
import static com.dremio.exec.ops.SnapshotDiffContext.NO_SNAPSHOT_DIFF;
import static com.dremio.exec.planner.physical.DmlPlanGeneratorBase.getHashDistributionTraitForFields;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.DELETE_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.IMPLICIT_SEQUENCE_NUMBER;
import static com.dremio.exec.store.SystemSchemas.POS;
import static com.dremio.exec.store.SystemSchemas.SEQUENCE_NUMBER;
import static com.dremio.exec.store.iceberg.IcebergUtils.READ_POSITIONAL_DELETE_JOIN_MODE_PROPERTY;
import static com.dremio.exec.store.iceberg.IcebergUtils.convertListTablePropertiesToMap;
import static com.dremio.exec.store.iceberg.IcebergUtils.hasEqualityDeletes;
import static com.dremio.exec.util.ColumnUtils.FILE_PATH_COLUMN_NAME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DelegatingTableMetadata;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.dfs.FilterableScan.PartitionStatsStatus;
import com.dremio.exec.store.iceberg.IcebergUtils.ReadPositionalDeleteJoinMode;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

public class IcebergScanPlanBuilder {

  private final IcebergScanPrel icebergScanPrel;

  public IcebergScanPlanBuilder(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      OptimizerRulesContext context,
      ManifestScanFilters manifestScanFilters,
      PruneFilterCondition pruneFilterCondition) {
    this.icebergScanPrel =
        new IcebergScanPrel(
            cluster,
            traitSet,
            table,
            tableMetadata.getStoragePluginId(),
            tableMetadata,
            projectedColumns,
            1.0,
            ImmutableList.of(),
            null,
            null,
            false,
            pruneFilterCondition,
            context,
            false,
            null,
            null,
            false,
            manifestScanFilters,
            NO_SNAPSHOT_DIFF,
            PartitionStatsStatus.NONE);
  }

  public IcebergScanPlanBuilder(IcebergScanPrel icebergScanPrel) {
    this.icebergScanPrel = icebergScanPrel;
  }

  protected IcebergScanPlanBuilder(IcebergScanPlanBuilder icebergScanPrel) {
    this.icebergScanPrel = icebergScanPrel.getIcebergScanPrel();
  }

  public static IcebergScanPlanBuilder fromDrel(
      ScanRelBase drel,
      OptimizerRulesContext context,
      boolean isArrowCachingEnabled,
      boolean canUsePartitionStats,
      boolean partitionValuesEnabled) {
    return fromDrel(
        drel,
        context,
        drel.getTableMetadata(),
        isArrowCachingEnabled,
        false,
        canUsePartitionStats,
        partitionValuesEnabled);
  }

  public static IcebergScanPlanBuilder fromDrel(
      ScanRelBase drel,
      OptimizerRulesContext context,
      TableMetadata tableMetadata,
      boolean isArrowCachingEnabled,
      boolean isConvertedIcebergDataset,
      boolean canUsePartitionStats,
      boolean partitionValuesEnabled) {
    FilterableScan filterableScan = (FilterableScan) drel;
    IcebergScanPrel prel =
        new IcebergScanPrel(
            drel.getCluster(),
            drel.getTraitSet().plus(Prel.PHYSICAL),
            drel.getTable(),
            drel.getPluginId(),
            tableMetadata,
            drel.getProjectedColumns(),
            drel.getObservedRowcountAdjustment(),
            drel.getHints(),
            filterableScan.getFilter(),
            filterableScan.getRowGroupFilter(),
            isArrowCachingEnabled,
            filterableScan.getPartitionFilter(),
            context,
            isConvertedIcebergDataset,
            filterableScan.getSurvivingRowCount(),
            filterableScan.getSurvivingFileCount(),
            canUsePartitionStats,
            ManifestScanFilters.empty(),
            drel.getSnapshotDiffContext(),
            partitionValuesEnabled,
            filterableScan.getPartitionStatsStatus());
    return new IcebergScanPlanBuilder(prel);
  }

  /**
   * This builds scan plans both with and without delete files. Without delete files, it simply
   * returns an IcebergScanPrel. See IcebergScanPrel.finalizeRel() for details on the expansion.
   * With delete files, the generated plan is as follows (details for pruning/filtering omitted for
   * brevity): DataFileScan | | exchange on split identity | | SplitGen | | DeleteFileAgg -
   * ARRAY_AGG(deletefile) GROUP BY datafile | | HashJoin
   * -----------------------------------------------| on specid, partitionkey, sequencenum(<=) | | |
   * | | | BroadcastExchange | | | | ManifestScan(DATA) no split gen ManifestScan(DELETES) no split
   * gen | | | | Exchange on split identity Exchange on split identity | | | |
   * ManifestListScan(DATA) ManifestListScan(DELETES)
   */
  public RelNode buildSingleSnapshotPlan() {
    RelNode output;
    if (hasDeleteFiles()) {
      if (!hasEqualityDeletes(icebergScanPrel.getTableMetadata())
          && icebergScanPrel
              .getContext()
              .getPlannerSettings()
              .getOptions()
              .getOption(ENABLE_READING_POSITIONAL_DELETE_WITH_ANTI_JOIN)) {
        // anti-join plan for positional deletes reading
        output = buildDataScanWithDeleteAntiJoin(icebergScanPrel.getContext());
      } else {
        output = buildManifestScanPlanWithDeletes(false);
        output = buildDataScanWithSplitGen(output);
      }
    } else {
      // no delete files, just return IcebergScanPrel which will get expanded in FinalizeRel stage
      output = icebergScanPrel;
    }

    return output;
  }

  /**
   * Generates the manifestScanPlan with delete files | DeleteFileAgg - ARRAY_AGG(deletefile) GROUP
   * BY datafile | | HashJoin -----------------------------------------------| on specid,
   * partitionkey, sequencenum(<=) | | | | | | BroadcastExchange | | | | ManifestScan(DATA) no split
   * gen ManifestScan(DELETES) no split gen | | | | Exchange on split identity Exchange on split
   * identity | | | | ManifestListScan(DATA) ManifestListScan(DELETES)
   */
  protected RelNode buildManifestScanPlanWithDeletes(boolean includeIcebergPartitionInfo) {
    RelNode output;
    RelNode data =
        icebergScanPrel.buildManifestScan(
            getDataManifestRecordCount(),
            new ImmutableManifestScanOptions.Builder()
                .setManifestContentType(ManifestContentType.DATA)
                .setIncludesIcebergPartitionInfo(includeIcebergPartitionInfo)
                .build());
    RelNode deletes =
        icebergScanPrel.buildManifestScan(
            getDeleteManifestRecordCount(),
            new ImmutableManifestScanOptions.Builder()
                .setManifestContentType(ManifestContentType.DELETES)
                .setIncludesIcebergPartitionInfo(includeIcebergPartitionInfo)
                .build());

    output = buildDataAndDeleteFileJoinAndAggregate(data, deletes);
    return output;
  }

  /**
   * Builds a plan for an Iceberg table scan We will take the
   * icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions() into consideration and if
   * needed we will limit the scan to data added/updated/delete between two or more snapshots
   *
   * @return a plan for an Iceberg table scan, possibly filtered by the FilterApplyOptions
   */
  public RelNode build() {
    if (icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions() == FILTER_DATA_FILES) {
      return createIcebergSnapshotBasedRefreshPlanBuilder()
          .buildSnapshotDiffFilterPlanAppendOnlyOptimized();
    } else if (icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions()
        == FILTER_PARTITIONS) {
      return createIcebergSnapshotBasedRefreshPlanBuilder().buildSnapshotDiffPlanFilterPartitions();
    } else if (icebergScanPrel.isPartitionValuesEnabled()) {
      return new IcebergPartitionAggregationPlanBuilder(icebergScanPrel)
          .buildManifestScanPlanForPartitionAggregation();
    }
    return buildSingleSnapshotPlan();
  }

  public IcebergSnapshotBasedRefreshPlanBuilder createIcebergSnapshotBasedRefreshPlanBuilder() {
    return new IcebergSnapshotBasedRefreshPlanBuilder(this);
  }

  /**
   * Scan data manifests and HashJoin on file paths from reading delete files. Consuming operation:
   * Selecting files to be optimized.
   *
   * <p>HashJoin -----------------------------------------------| on path, TODO: sequencenum(<=) | |
   * | | HashAgg | | | DataFileScan | | ManifestListScan(DATA) ManifestListScan(DELETES)
   */
  public RelNode buildDataManifestScanWithDeleteJoin(RelNode delete) {
    RelOptCluster cluster = icebergScanPrel.getCluster();

    ManifestScanOptions manifestScanOptions =
        new ImmutableManifestScanOptions.Builder()
            .setIncludesSplitGen(false)
            .setManifestContentType(ManifestContentType.DATA)
            .setIncludesIcebergMetadata(true)
            .build();

    RelNode manifestScan = buildManifestRel(manifestScanOptions, false);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    Pair<Integer, RelDataTypeField> dataFilePathCol =
        MoreRelOptUtil.findFieldWithIndex(manifestScan.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> deleteDataFilePathCol =
        MoreRelOptUtil.findFieldWithIndex(delete.getRowType().getFieldList(), DELETE_FILE_PATH);
    Pair<Integer, RelDataTypeField> dataFileSeqNoCol =
        MoreRelOptUtil.findFieldWithIndex(
            manifestScan.getRowType().getFieldList(), SEQUENCE_NUMBER);
    Pair<Integer, RelDataTypeField> deleteFileSeqNoCol =
        MoreRelOptUtil.findFieldWithIndex(
            delete.getRowType().getFieldList(), IMPLICIT_SEQUENCE_NUMBER);
    int probeFieldCount = manifestScan.getRowType().getFieldCount();
    RexNode joinCondition =
        rexBuilder.makeCall(
            EQUALS,
            rexBuilder.makeInputRef(dataFilePathCol.right.getType(), dataFilePathCol.left),
            rexBuilder.makeInputRef(
                deleteDataFilePathCol.right.getType(),
                probeFieldCount + deleteDataFilePathCol.left));
    RexNode extraJoinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(dataFileSeqNoCol.right.getType(), dataFileSeqNoCol.left),
            rexBuilder.makeInputRef(
                deleteFileSeqNoCol.right.getType(), probeFieldCount + deleteFileSeqNoCol.left));

    return HashJoinPrel.create(
        cluster,
        manifestScan.getTraitSet(),
        manifestScan,
        delete,
        joinCondition,
        extraJoinCondition,
        JoinRelType.LEFT,
        true);
  }

  /** This builds manifest scan plans both with and without delete files. */
  public RelNode buildManifestRel(ManifestScanOptions manifestScanOptions) {
    return buildManifestRel(manifestScanOptions, true);
  }

  /**
   * This builds manifest scan plans (With both data and delete files if combineScan is set to true
   * , else for manifests that fit the {@code manifestScanOptions}).
   */
  public RelNode buildManifestRel(ManifestScanOptions manifestScanOptions, boolean combineScan) {
    if (combineScan) {
      RelNode output =
          icebergScanPrel.buildManifestScan(
              getDataManifestRecordCount(),
              new ImmutableManifestScanOptions.Builder()
                  .from(manifestScanOptions)
                  .setManifestContentType(ManifestContentType.DATA)
                  .build());

      if (hasDeleteFiles()) {
        RelNode deletes =
            icebergScanPrel.buildManifestScan(
                getDataManifestRecordCount(),
                new ImmutableManifestScanOptions.Builder()
                    .from(manifestScanOptions)
                    .setManifestContentType(ManifestContentType.DELETES)
                    .build());
        output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
      }

      return output;
    } else {
      return icebergScanPrel.buildManifestScan(getDataManifestRecordCount(), manifestScanOptions);
    }
  }

  public RelNode buildWithDmlDataFileFiltering(RelNode dataFileFilterList) {
    ManifestScanOptions manifestScanOptions =
        new ImmutableManifestScanOptions.Builder()
            .setManifestContentType(ManifestContentType.DATA)
            .setIncludesSplitGen(false)
            .build();
    RelNode output =
        icebergScanPrel.buildManifestScan(getDataManifestRecordCount(), manifestScanOptions);
    // join with the unique data files to filter
    output = buildDataFileFilteringSemiJoin(output, dataFileFilterList);

    if (hasDeleteFiles()) {
      // if the table has delete files, add a delete manifest scan and join/aggregate applicable
      // delete files to
      // data files
      manifestScanOptions =
          new ImmutableManifestScanOptions.Builder()
              .from(manifestScanOptions)
              .setManifestContentType(ManifestContentType.DELETES)
              .build();
      RelNode deletes =
          icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(), manifestScanOptions);
      output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
    }

    // perform split gen
    return buildDataScanWithSplitGen(output);
  }

  protected RelNode buildSplitGen(RelNode input) {
    BatchSchema splitGenOutputSchema =
        input.getRowType().getField(SystemSchemas.DELETE_FILES, false, false) == null
            ? SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA
            : ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA;

    // perform split generation
    return new IcebergSplitGenPrel(
        input.getCluster(),
        input.getTraitSet(),
        icebergScanPrel.getTable(),
        input,
        icebergScanPrel.getTableMetadata(),
        splitGenOutputSchema,
        icebergScanPrel.isConvertedIcebergDataset());
  }

  private RelNode buildSingleSplitGenWithFilePathOutput(RelNode input) {
    BatchSchema splitGenOutputSchema =
        BatchSchema.newBuilder()
            .addFields(RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.getFields())
            .addField(
                Field.nullable(SystemSchemas.DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
            .build();

    // perform split generation
    return new IcebergSplitGenPrel(
        input.getCluster(),
        input.getTraitSet(),
        icebergScanPrel.getTable(),
        input,
        icebergScanPrel.getTableMetadata(),
        splitGenOutputSchema,
        icebergScanPrel.isConvertedIcebergDataset(),
        true);
  }

  public RelNode buildDataScanWithSplitGen(RelNode input) {
    RelNode output = buildSplitGen(input);
    return icebergScanPrel.buildDataFileScan(output, false);
  }

  public static ReadPositionalDeleteJoinMode getReadPositionalDeleteJoinMode(
      final TableMetadata tableMetadata) {
    return Optional.ofNullable(tableMetadata)
        .map(TableMetadata::getDatasetConfig)
        .map(DatasetConfig::getPhysicalDataset)
        .map(PhysicalDataset::getIcebergMetadata)
        .map(IcebergMetadata::getTablePropertiesList)
        .map(props -> convertListTablePropertiesToMap(props))
        .map(props -> props.get(READ_POSITIONAL_DELETE_JOIN_MODE_PROPERTY))
        .map(
            value ->
                value.equalsIgnoreCase(ReadPositionalDeleteJoinMode.BROADCAST.value())
                    ? ReadPositionalDeleteJoinMode.BROADCAST
                    : ReadPositionalDeleteJoinMode.DEFAULT)
        .orElse(ReadPositionalDeleteJoinMode.DEFAULT);
  }

  private RelNode buildHashExchangeOnFilePathField(RelNode input, String filePathFieldName) {
    DistributionTrait dataDistributionTrait =
        getHashDistributionTraitForFields(input.getRowType(), ImmutableList.of(filePathFieldName));
    RelTraitSet dataTraitSet =
        input
            .getCluster()
            .getPlanner()
            .emptyTraitSet()
            .plus(Prel.PHYSICAL)
            .plus(dataDistributionTrait);
    return new HashToRandomExchangePrel(
        input.getCluster(), dataTraitSet, input, dataDistributionTrait.getFields());
  }

  /***
   * Anti-join based approach to read table with delete files. There are two variants:
   * 1. Disable splitting data file scans on data file side,
   *    Thus, we can also eliminate any exchange after the data file scan
   *    and just have the positional deletes routed to the data file nodes - using the split hash based on the data file path
   * 2. When user specifies a table property "positional_delete.force.broadcast.delete_data",
   *    delete file rows will be broadcasted to all data file scan threads to perform the join
   *    Data file scan side would allow file split
   */
  public RelNode buildDataScanWithDeleteAntiJoin(OptimizerRulesContext context) {
    // data file scan side
    RelNode dataManifestScan =
        icebergScanPrel.buildManifestScan(
            getDataManifestRecordCount(),
            new ImmutableManifestScanOptions.Builder()
                .setManifestContentType(ManifestContentType.DATA)
                .setIncludesIcebergPartitionInfo(false)
                .build());
    RelNode data;
    ReadPositionalDeleteJoinMode readPositionalDeleteJoinMode =
        getReadPositionalDeleteJoinMode(icebergScanPrel.getTableMetadata());
    if (readPositionalDeleteJoinMode == ReadPositionalDeleteJoinMode.BROADCAST) {
      // When user specifies "broadcast" mode in table property
      // "dremio.read.positional_delete_join_mode",
      // delete file rows will be broadcasted to all data file scan threads to perform the join
      // Data file scan side would allow file split
      RelNode splitGen = buildSplitGen(dataManifestScan);
      data = icebergScanPrel.buildDataFileScan(splitGen, true);
    } else {
      // Disable splitting data file scans on data file side
      data = buildSingleSplitGenWithFilePathOutput(dataManifestScan);
      // hash distribute data file based on the data file path
      RelNode dataSideHashExchange =
          buildHashExchangeOnFilePathField(data, SystemSchemas.DATAFILE_PATH);
      data =
          icebergScanPrel.buildDataFileScanTableFunction(
              dataSideHashExchange, Collections.EMPTY_LIST, true);
    }

    // delete file scan side
    BatchSchema posDeleteFileSchema =
        BatchSchema.newBuilder()
            .addField(Field.nullable(DELETE_FILE_PATH, Types.MinorType.VARCHAR.getType()))
            .addField(Field.nullable(POS, Types.MinorType.BIGINT.getType()))
            .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
            .build();
    Prel deletes = buildDeleteFileScan(context, posDeleteFileSchema);

    RelNode deleteExchange;
    if (readPositionalDeleteJoinMode == ReadPositionalDeleteJoinMode.BROADCAST) {
      // broadcast delete rows to all data file scan threads
      deleteExchange =
          new BroadcastExchangePrel(deletes.getCluster(), deletes.getTraitSet(), deletes);
    } else {
      // have the positional deletes routed to the data file nodes - using the split hash based on
      // the data file path
      deleteExchange = buildHashExchangeOnFilePathField(deletes, DELETE_FILE_PATH);
    }

    // build anti-join between data file scan and delete file scan on file path
    return antiJoinDataAndDeleteScan(data, deleteExchange);
  }

  private Prel antiJoinDataAndDeleteScan(RelNode data, RelNode deletes) {
    // hash join on __FilePath == __FilePath AND __RowIndex == __RowIndex
    RelDataTypeField leftFilePathField =
        data.getRowType().getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    RelDataTypeField leftRowIndexField =
        data.getRowType().getField(ColumnUtils.ROW_INDEX_COLUMN_NAME, false, false);
    RelDataTypeField rightFilePathField =
        deletes.getRowType().getField(DELETE_FILE_PATH, false, false);
    RelDataTypeField rightRowIndexField = deletes.getRowType().getField(POS, false, false);

    int leftFieldCount = data.getRowType().getFieldCount();
    RexBuilder rexBuilder = data.getCluster().getRexBuilder();
    RexNode joinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(leftFilePathField.getType(), leftFilePathField.getIndex()),
                rexBuilder.makeInputRef(
                    rightFilePathField.getType(), leftFieldCount + rightFilePathField.getIndex())),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(leftRowIndexField.getType(), leftRowIndexField.getIndex()),
                rexBuilder.makeInputRef(
                    rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex())));

    Prel leftJoin =
        HashJoinPrel.create(
            data.getCluster(),
            data.getTraitSet(),
            data,
            deletes,
            joinCondition,
            null,
            JoinRelType.LEFT,
            true);

    // anti join by filtering out matched rows from the left join
    RelDataTypeField filePathFieldInDeleteFile =
        leftJoin.getRowType().getField(DELETE_FILE_PATH, false, false);

    RexNode filterCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(
                filePathFieldInDeleteFile.getType(), filePathFieldInDeleteFile.getIndex()));

    Prel antiJoin =
        FilterPrel.create(leftJoin.getCluster(), leftJoin.getTraitSet(), leftJoin, filterCondition);

    // filepath and rowIndex columns from data branch side has two possible sources:
    // 1. they are added in IcebergScanPrel for tables with delete files
    //    we need to remove them from IcebergScan plan output since they are only available inside
    // IcebergScan sub-plan
    // 2. they could be already extended during SqlNode phase for DML target tables
    //    we need to keep them from IcebergScan plan output since they are required by DML plans
    int numSystemColumnsToRemove =
        ((TableFunctionPrel) data).getTableMetadata().getSchema().getFieldCount()
            - data.getTable().getRowType().getFieldCount();

    List<RelDataTypeField> projectFields =
        antiJoin
            .getRowType()
            .getFieldList()
            .subList(0, data.getRowType().getFieldCount() - numSystemColumnsToRemove);

    List<String> projectNames =
        projectFields.stream().map(RelDataTypeField::getName).collect(Collectors.toList());

    List<RexNode> projectExprs =
        projectFields.stream()
            .map(f -> rexBuilder.makeInputRef(f.getType(), f.getIndex()))
            .collect(Collectors.toList());

    RelDataType projectRowType =
        RexUtil.createStructType(
            antiJoin.getCluster().getTypeFactory(), projectExprs, projectNames, null);

    return ProjectPrel.create(
        antiJoin.getCluster(), antiJoin.getTraitSet(), antiJoin, projectExprs, projectRowType);
  }

  public RelNode buildDataAndDeleteFileJoinAndAggregate(RelNode data, RelNode deletes) {
    // put delete files on the build side... we will always broadcast as regular table maintenance
    // is assumed to keep
    // delete file counts at a reasonable level
    RelOptCluster cluster = icebergScanPrel.getCluster();
    RelNode build = new BroadcastExchangePrel(cluster, deletes.getTraitSet(), deletes);

    RelDataTypeField probeSeqNumField = getField(data, SystemSchemas.SEQUENCE_NUMBER);
    RelDataTypeField probeSpecIdField = getField(data, SystemSchemas.PARTITION_SPEC_ID);
    RelDataTypeField probePartKeyField = getField(data, SystemSchemas.PARTITION_KEY);
    RelDataTypeField buildSeqNumField = getField(build, SystemSchemas.SEQUENCE_NUMBER);
    RelDataTypeField buildSpecIdField = getField(build, SystemSchemas.PARTITION_SPEC_ID);
    RelDataTypeField buildPartKeyField = getField(build, SystemSchemas.PARTITION_KEY);

    // build the join condition:
    //   data.partitionSpecId == deletes.partitionSpecId &&
    //   data.partitionKey == deletes.partitionKey
    // with extra condition:
    //   data.sequenceNumber <= deletes.sequenceNumber
    int probeFieldCount = data.getRowType().getFieldCount();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode joinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(probeSpecIdField.getType(), probeSpecIdField.getIndex()),
                rexBuilder.makeInputRef(
                    buildSpecIdField.getType(), probeFieldCount + buildSpecIdField.getIndex())),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(probePartKeyField.getType(), probePartKeyField.getIndex()),
                rexBuilder.makeInputRef(
                    buildPartKeyField.getType(), probeFieldCount + buildPartKeyField.getIndex())));
    RexNode extraJoinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            rexBuilder.makeInputRef(probeSeqNumField.getType(), probeSeqNumField.getIndex()),
            rexBuilder.makeInputRef(
                buildSeqNumField.getType(), probeFieldCount + buildSeqNumField.getIndex()));

    RelNode output =
        HashJoinPrel.create(
            cluster,
            data.getTraitSet(),
            data,
            build,
            joinCondition,
            extraJoinCondition,
            JoinRelType.LEFT,
            true);

    // project the following columns going into the aggregate:
    //   data.datafilePath
    //   data.fileSize
    //   data.partitionInfo
    //   data.colIds
    //   deletes.deleteFile
    //   data.partitionSpecId
    List<RexNode> exprs =
        ImmutableList.of(
            rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.DATAFILE_PATH)),
            rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.FILE_SIZE)),
            rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.PARTITION_INFO)),
            rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.COL_IDS)),
            rexBuilder.makeInputRef(
                output,
                data.getRowType().getFieldCount()
                    + getFieldIndex(deletes, SystemSchemas.DELETE_FILE)),
            rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.PARTITION_SPEC_ID)));
    List<String> names =
        ImmutableList.of(
            SystemSchemas.DATAFILE_PATH,
            SystemSchemas.FILE_SIZE,
            SystemSchemas.PARTITION_INFO,
            SystemSchemas.COL_IDS,
            SystemSchemas.DELETE_FILE,
            SystemSchemas.PARTITION_SPEC_ID);
    RelDataType projectType =
        RexUtil.createStructType(cluster.getTypeFactory(), exprs, names, null);
    output = ProjectPrel.create(cluster, output.getTraitSet(), output, exprs, projectType);

    // aggregate joined rows to get a single row per data file with the associated list of delete
    // files, with
    // rowcount estimate based on number of data files going into the join above
    Double dataFileCountEstimate = cluster.getMetadataQuery().getRowCount(data);
    return new IcebergDeleteFileAggPrel(
        cluster,
        output.getTraitSet(),
        icebergScanPrel.getTable(),
        output,
        icebergScanPrel.getTableMetadata(),
        dataFileCountEstimate == null ? null : dataFileCountEstimate.longValue());
  }

  /**
   * HashJoinPrel ----------------------------------------------| INNER ON datafilePath ==
   * __FilePath | | | | BroadcastExchangePrel | | inputDataFiles dataFileFilterList from manifest
   * scan from DmlPlanGenerator.createDataFileAggPrel()
   */
  private Prel buildDataFileFilteringSemiJoin(RelNode inputDataFiles, RelNode dataFileFilterList) {

    // rename __FilePath column coming from dataFileFilterList
    // the auto-rename logic isn't working in the join below and we end up with dupe column name
    // assertions
    String filePathForCompare = FILE_PATH_COLUMN_NAME + "__ForCompare";
    RexBuilder rexBuilder = inputDataFiles.getCluster().getRexBuilder();
    RelDataTypeField projectField =
        dataFileFilterList.getRowType().getField(FILE_PATH_COLUMN_NAME, false, false);
    List<String> projectNames = ImmutableList.of(filePathForCompare);
    List<RexNode> projectExprs =
        ImmutableList.of(rexBuilder.makeInputRef(projectField.getType(), projectField.getIndex()));
    RelDataType projectRowType =
        RexUtil.createStructType(
            dataFileFilterList.getCluster().getTypeFactory(), projectExprs, projectNames, null);
    ProjectPrel rightDataFileListInputProject =
        ProjectPrel.create(
            dataFileFilterList.getCluster(),
            dataFileFilterList.getTraitSet(),
            dataFileFilterList,
            projectExprs,
            projectRowType);

    // build side of join
    // - use broadcast join
    // - we don't have the row count info of unique DMLed files (i.e., after Agg).
    // - so we can't make the choice between BroadcastExchange vs HashToRandomExchange
    BroadcastExchangePrel broadcastExchange =
        new BroadcastExchangePrel(
            rightDataFileListInputProject.getCluster(),
            rightDataFileListInputProject.getTraitSet(),
            rightDataFileListInputProject);

    // manifestScanTF INNER JOIN dataFileListInput
    // ON manifestScanTF.datafilePath == dataFileListInput.__FilePath
    RelDataTypeField pathField =
        inputDataFiles.getRowType().getField(SystemSchemas.DATAFILE_PATH, false, false);
    RexNode joinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(pathField.getType(), pathField.getIndex()),
            rexBuilder.makeInputRef(
                broadcastExchange.getRowType().getFieldList().get(0).getType(),
                inputDataFiles.getRowType().getFieldCount()));

    // join to restrict data file scan to data files present in dataFileListInput
    // do we need to remove the extra __FilePath col after this? just ignore for now
    return HashJoinPrel.create(
        inputDataFiles.getCluster(),
        inputDataFiles.getTraitSet(),
        inputDataFiles,
        broadcastExchange,
        joinCondition,
        null,
        JoinRelType.INNER,
        true);
  }

  /**
   * Builds Manifest and File Scan for positional delete files Consuming operation: Optimize with
   * positional deletes - Delete File scan is needed to determine which data files have positional
   * deletes linked to them and subsequently, need to be rewritten.
   *
   * <p>DataFileScan | | exchange on split identity | | SplitGen | | ManifestScan(DELETE) | |
   * Exchange on split identity | | ManifestListScan(DELETE)
   */
  public Prel buildDeleteFileScan(OptimizerRulesContext context, BatchSchema posDeleteFileSchema) {
    DelegatingTableMetadata deleteFileTableMetadata =
        new DelegatingTableMetadata(icebergScanPrel.getTableMetadata()) {
          @Override
          public BatchSchema getSchema() {
            return posDeleteFileSchema;
          }
        };
    IcebergScanPrel deleteFileScanPrel =
        new IcebergScanPrel(
            icebergScanPrel.getCluster(),
            icebergScanPrel.getTraitSet(),
            icebergScanPrel.getTable(),
            deleteFileTableMetadata.getStoragePluginId(),
            deleteFileTableMetadata,
            posDeleteFileSchema.getFields().stream()
                .map(i -> SchemaPath.getSimplePath(i.getName()))
                .collect(Collectors.toList()),
            1.0,
            ImmutableList.of(),
            null,
            null,
            false,
            null,
            context,
            false,
            null,
            null,
            false,
            ImmutableManifestScanFilters.empty(),
            NO_SNAPSHOT_DIFF,
            icebergScanPrel.getPartitionStatsStatus());

    ManifestScanOptions deleteManifestScanOptions =
        new ImmutableManifestScanOptions.Builder()
            .setIncludesSplitGen(false)
            .setManifestContentType(ManifestContentType.DELETES)
            .setIncludesIcebergMetadata(true)
            .build();
    RelNode manifestDeleteScan =
        icebergScanPrel.buildManifestScan(
            getDeleteManifestRecordCount(), deleteManifestScanOptions);
    manifestDeleteScan =
        new IcebergSplitGenPrel(
            manifestDeleteScan.getCluster(),
            manifestDeleteScan.getTraitSet(),
            icebergScanPrel.getTable(),
            manifestDeleteScan,
            deleteFileTableMetadata,
            SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
            icebergScanPrel.isConvertedIcebergDataset());
    return deleteFileScanPrel.buildDataFileScanWithImplicitPartitionCols(
        manifestDeleteScan, ImmutableList.of(IMPLICIT_SEQUENCE_NUMBER), false);
  }

  protected static RelDataTypeField getField(RelNode rel, String fieldName) {
    return rel.getRowType().getField(fieldName, false, false);
  }

  private static int getFieldIndex(RelNode rel, String fieldName) {
    return rel.getRowType().getField(fieldName, false, false).getIndex();
  }

  protected long getDataManifestRecordCount() {
    return icebergScanPrel.getSurvivingFileCount();
  }

  private long getDeleteRecordCount() {
    final ScanStats deleteStats =
        icebergScanPrel
            .getTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getDeleteStats();
    return deleteStats != null ? deleteStats.getRecordCount() : 0;
  }

  protected long getDeleteManifestRecordCount() {
    final ScanStats deleteManifestStats =
        icebergScanPrel
            .getTableMetadata()
            .getDatasetConfig()
            .getPhysicalDataset()
            .getIcebergMetadata()
            .getDeleteManifestStats();
    return deleteManifestStats != null ? deleteManifestStats.getRecordCount() : 0;
  }

  public boolean hasDeleteFiles() {
    return getDeleteRecordCount() > 0;
  }

  protected IcebergScanPrel getIcebergScanPrel() {
    return icebergScanPrel;
  }
}
