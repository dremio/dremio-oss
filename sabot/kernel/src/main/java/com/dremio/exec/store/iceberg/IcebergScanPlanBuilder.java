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

import static com.dremio.exec.ops.SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES;
import static com.dremio.exec.ops.SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS;
import static com.dremio.exec.ops.SnapshotDiffContext.NO_SNAPSHOT_DIFF;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.DELETE_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_POS_DELETE_FILE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.IMPLICIT_SEQUENCE_NUMBER;
import static com.dremio.exec.store.SystemSchemas.SEQUENCE_NUMBER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;

import java.util.List;
import java.util.stream.Collectors;

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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.partition.PruneFilterCondition;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.DelegatingTableMetadata;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.google.common.collect.ImmutableList;

public class IcebergScanPlanBuilder{

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
    this.icebergScanPrel = new IcebergScanPrel(
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
      NO_SNAPSHOT_DIFF);
  }

  public IcebergScanPlanBuilder(
      IcebergScanPrel icebergScanPrel) {
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
    boolean partitionValuesEnabled
  ) {
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
    boolean partitionValuesEnabled
  ) {
    FilterableScan filterableScan = (FilterableScan) drel;
    IcebergScanPrel prel = new IcebergScanPrel(
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
      partitionValuesEnabled);
    return new IcebergScanPlanBuilder(prel);
  }

  /**
   * This builds scan plans both with and without delete files.
   * Without delete files, it simply returns an IcebergScanPrel.  See IcebergScanPrel.finalizeRel() for details
   * on the expansion.
   * With delete files, the generated plan is as follows (details for pruning/filtering omitted for brevity):
   * DataFileScan
   *   |
   *   |
   * exchange on split identity
   *   |
   *   |
   * SplitGen
   *   |
   *   |
   * DeleteFileAgg - ARRAY_AGG(deletefile) GROUP BY datafile
   *   |
   *   |
   * HashJoin -----------------------------------------------|
   * on specid, partitionkey, sequencenum(<=)                |
   *   |                                                     |
   *   |                                                     |
   *   |                                                   BroadcastExchange
   *   |                                                     |
   *   |                                                     |
   * ManifestScan(DATA) no split gen                       ManifestScan(DELETES) no split gen
   *   |                                                     |
   *   |                                                     |
   * Exchange on split identity                            Exchange on split identity
   *   |                                                     |
   *   |                                                     |
   * ManifestListScan(DATA)                                ManifestListScan(DELETES)
   */
  public RelNode buildSingleSnapshotPlan() {
    RelNode output;
    if (hasDeleteFiles()) {
      output = buildManifestScanPlanWithDeletes(false);
      output = buildDataScanWithSplitGen(output);
    } else {
      // no delete files, just return IcebergScanPrel which will get expanded in FinalizeRel stage
      output = icebergScanPrel;
    }

    return output;
  }

  /** Generates the manifestScanPlan with delete files
   *   |
   * DeleteFileAgg - ARRAY_AGG(deletefile) GROUP BY datafile
   *   |
   *   |
   * HashJoin -----------------------------------------------|
   * on specid, partitionkey, sequencenum(<=)                |
   *   |                                                     |
   *   |                                                     |
   *   |                                                   BroadcastExchange
   *   |                                                     |
   *   |                                                     |
   * ManifestScan(DATA) no split gen                       ManifestScan(DELETES) no split gen
   *   |                                                     |
   *   |                                                     |
   * Exchange on split identity                            Exchange on split identity
   *   |                                                     |
   *   |                                                     |
   * ManifestListScan(DATA)                                ManifestListScan(DELETES)
   */
  protected RelNode buildManifestScanPlanWithDeletes(boolean includeIcebergPartitionInfo) {
    RelNode output;
    RelNode data = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
      new ImmutableManifestScanOptions.Builder()
        .setManifestContentType(ManifestContentType.DATA)
        .setIncludesIcebergPartitionInfo(includeIcebergPartitionInfo)
        .build());
    RelNode deletes = icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(),
      new ImmutableManifestScanOptions.Builder()
        .setManifestContentType(ManifestContentType.DELETES)
        .setIncludesIcebergPartitionInfo(includeIcebergPartitionInfo)
        .build());

    output = buildDataAndDeleteFileJoinAndAggregate(data, deletes);
    return output;
  }

  /**
   * Builds a plan for an Iceberg table scan
   * We will take the icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions() into consideration
   * and if needed we will limit the scan to data added/updated/delete between two or more snapshots
   * @return a plan for an Iceberg table scan, possibly filtered by the FilterApplyOptions
   */
  public RelNode build() {
    if(icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions() == FILTER_DATA_FILES){
      return createIcebergSnapshotBasedRefreshPlanBuilder().buildSnapshotDiffFilterPlanAppendOnlyOptimized();
    } else if(icebergScanPrel.getSnapshotDiffContext().getFilterApplyOptions() == FILTER_PARTITIONS){
      return createIcebergSnapshotBasedRefreshPlanBuilder().buildSnapshotDiffPlanFilterPartitions();
    } else if(icebergScanPrel.isPartitionValuesEnabled()){
      return new IcebergPartitionAggregationPlanBuilder(icebergScanPrel)
        .buildManifestScanPlanForPartitionAggregation();
    }
    return buildSingleSnapshotPlan();
  }

  public IcebergSnapshotBasedRefreshPlanBuilder createIcebergSnapshotBasedRefreshPlanBuilder(){
    return new IcebergSnapshotBasedRefreshPlanBuilder(this);
  }

  /**
   * Scan data manifests and HashJoin on file paths from reading delete files.
   * Consuming operation: Selecting files to be optimized.
   *
   * HashJoin -----------------------------------------------|
   * on path, TODO: sequencenum(<=)                          |
   *   |                                                     |
   *   |                                                   HashAgg
   *   |                                                     |
   *   |                                                   DataFileScan
   *   |                                                     |
   * ManifestListScan(DATA)                                ManifestListScan(DELETES)
   */
  public RelNode buildDataManifestScanWithDeleteJoin(RelNode delete) {
    RelOptCluster cluster = icebergScanPrel.getCluster();

    ManifestScanOptions manifestScanOptions = new ImmutableManifestScanOptions.Builder()
      .setIncludesSplitGen(false)
      .setManifestContentType(ManifestContentType.DATA)
      .setIncludesIcebergMetadata(true)
      .build();

    RelNode manifestScan = buildManifestRel(manifestScanOptions, false);
    RexBuilder rexBuilder = cluster.getRexBuilder();

    Pair<Integer, RelDataTypeField> dataFilePathCol = MoreRelOptUtil.findFieldWithIndex(manifestScan.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> deleteDataFilePathCol = MoreRelOptUtil.findFieldWithIndex(delete.getRowType().getFieldList(), DELETE_FILE_PATH);
    Pair<Integer, RelDataTypeField> dataFileSeqNoCol = MoreRelOptUtil.findFieldWithIndex(manifestScan.getRowType().getFieldList(), SEQUENCE_NUMBER);
    Pair<Integer, RelDataTypeField> deleteFileSeqNoCol = MoreRelOptUtil.findFieldWithIndex(delete.getRowType().getFieldList(), IMPLICIT_SEQUENCE_NUMBER);
    int probeFieldCount = manifestScan.getRowType().getFieldCount();
    RexNode joinCondition = rexBuilder.makeCall(
      EQUALS,
      rexBuilder.makeInputRef(dataFilePathCol.right.getType(), dataFilePathCol.left),
      rexBuilder.makeInputRef(deleteDataFilePathCol.right.getType(), probeFieldCount + deleteDataFilePathCol.left));
    RexNode extraJoinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(dataFileSeqNoCol.right.getType(), dataFileSeqNoCol.left),
      rexBuilder.makeInputRef(deleteFileSeqNoCol.right.getType(), probeFieldCount + deleteFileSeqNoCol.left));

    return HashJoinPrel.create(cluster, manifestScan.getTraitSet(), manifestScan, delete, joinCondition, extraJoinCondition, JoinRelType.LEFT);
  }

  /**
   * This builds manifest scan plans both with and without delete files.
   */
  public RelNode buildManifestRel(ManifestScanOptions manifestScanOptions) {
    return buildManifestRel(manifestScanOptions, true);
  }

  /**
   * This builds manifest scan plans (With both data and delete files if combineScan is set to true
   * , else for manifests that fit the {@code manifestScanOptions}).
   */
  public RelNode buildManifestRel(ManifestScanOptions manifestScanOptions, boolean combineScan) {
    if (combineScan) {
      RelNode output = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
        new ImmutableManifestScanOptions.Builder().from(manifestScanOptions).setManifestContentType(ManifestContentType.DATA).build());

      if (hasDeleteFiles()) {
        RelNode deletes = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
          new ImmutableManifestScanOptions.Builder().from(manifestScanOptions).setManifestContentType(ManifestContentType.DELETES).build());
        output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
      }

      return output;
    } else {
      return icebergScanPrel.buildManifestScan(getDataManifestRecordCount(), manifestScanOptions);
    }
  }

  public RelNode buildWithDmlDataFileFiltering(RelNode dataFileFilterList) {
    ManifestScanOptions manifestScanOptions =  new ImmutableManifestScanOptions.Builder()
      .setManifestContentType(ManifestContentType.DATA).setIncludesSplitGen(false).build();
    RelNode output = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(), manifestScanOptions);
    // join with the unique data files to filter
    output = buildDataFileFilteringSemiJoin(output, dataFileFilterList);

    if (hasDeleteFiles()) {
      // if the table has delete files, add a delete manifest scan and join/aggregate applicable delete files to
      // data files
      manifestScanOptions = new ImmutableManifestScanOptions.Builder().from(manifestScanOptions)
        .setManifestContentType(ManifestContentType.DELETES).build();
      RelNode deletes = icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(), manifestScanOptions);
      output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
    }

    // perform split gen
    return buildDataScanWithSplitGen(output);
  }

  protected RelNode buildSplitGen(RelNode input) {
    BatchSchema splitGenOutputSchema =
      input.getRowType().getField(SystemSchemas.DELETE_FILES, false, false) == null ?
        SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA :
        ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA;

    // perform split generation
    return new IcebergSplitGenPrel(input.getCluster(), input.getTraitSet(), icebergScanPrel.getTable(), input,
      icebergScanPrel.getTableMetadata(), splitGenOutputSchema, icebergScanPrel.isConvertedIcebergDataset());
  }

  public RelNode buildDataScanWithSplitGen(RelNode input) {
    RelNode output = buildSplitGen(input);
    return icebergScanPrel.buildDataFileScan(output);
  }

  public RelNode buildDataAndDeleteFileJoinAndAggregate(RelNode data, RelNode deletes) {
    // put delete files on the build side... we will always broadcast as regular table maintenance is assumed to keep
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
    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.AND,
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(probeSpecIdField.getType(), probeSpecIdField.getIndex()),
        rexBuilder.makeInputRef(buildSpecIdField.getType(), probeFieldCount + buildSpecIdField.getIndex())),
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(probePartKeyField.getType(), probePartKeyField.getIndex()),
        rexBuilder.makeInputRef(buildPartKeyField.getType(), probeFieldCount + buildPartKeyField.getIndex())));
    RexNode extraJoinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(probeSeqNumField.getType(), probeSeqNumField.getIndex()),
      rexBuilder.makeInputRef(buildSeqNumField.getType(), probeFieldCount + buildSeqNumField.getIndex()));

    RelNode output = HashJoinPrel.create(cluster, data.getTraitSet(), data, build, joinCondition, extraJoinCondition,
      JoinRelType.LEFT);

    // project the following columns going into the aggregate:
    //   data.datafilePath
    //   data.fileSize
    //   data.partitionInfo
    //   data.colIds
    //   deletes.deleteFile
    //   data.partitionSpecId
    List<RexNode> exprs = ImmutableList.of(
      rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.DATAFILE_PATH)),
      rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.FILE_SIZE)),
      rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.PARTITION_INFO)),
      rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.COL_IDS)),
      rexBuilder.makeInputRef(output, data.getRowType().getFieldCount() +
        getFieldIndex(deletes, SystemSchemas.DELETE_FILE)),
      rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.PARTITION_SPEC_ID)));
    List<String> names = ImmutableList.of(
      SystemSchemas.DATAFILE_PATH,
      SystemSchemas.FILE_SIZE,
      SystemSchemas.PARTITION_INFO,
      SystemSchemas.COL_IDS,
      SystemSchemas.DELETE_FILE,
      SystemSchemas.PARTITION_SPEC_ID);
    RelDataType projectType = RexUtil.createStructType(cluster.getTypeFactory(), exprs, names, null);
    output = ProjectPrel.create(cluster, output.getTraitSet(), output, exprs, projectType);

    // aggregate joined rows to get a single row per data file with the associated list of delete files, with
    // rowcount estimate based on number of data files going into the join above
    Double dataFileCountEstimate = cluster.getMetadataQuery().getRowCount(data);
    return new IcebergDeleteFileAggPrel(cluster, output.getTraitSet(), icebergScanPrel.getTable(), output,
      icebergScanPrel.getTableMetadata(), dataFileCountEstimate == null ? null : dataFileCountEstimate.longValue());
  }

  /**
   *    HashJoinPrel ----------------------------------------------|
   *    INNER ON datafilePath == __FilePath                        |
   *        |                                                      |
   *        |                                                  BroadcastExchangePrel
   *        |                                                      |
   *    inputDataFiles                                         dataFileFilterList
   *    from manifest scan                                     from DmlPlanGenerator.createDataFileAggPrel()
   */
  private Prel buildDataFileFilteringSemiJoin(RelNode inputDataFiles, RelNode dataFileFilterList) {

    // rename __FilePath column coming from dataFileFilterList
    // the auto-rename logic isn't working in the join below and we end up with dupe column name assertions
    String filePathForCompare = ColumnUtils.FILE_PATH_COLUMN_NAME + "__ForCompare";
    RexBuilder rexBuilder = inputDataFiles.getCluster().getRexBuilder();
    RelDataTypeField projectField = dataFileFilterList.getRowType().getField(ColumnUtils.FILE_PATH_COLUMN_NAME, false, false);
    List<String> projectNames = ImmutableList.of(filePathForCompare);
    List<RexNode> projectExprs = ImmutableList.of(rexBuilder.makeInputRef(projectField.getType(),
      projectField.getIndex()));
    RelDataType projectRowType = RexUtil.createStructType(dataFileFilterList.getCluster().getTypeFactory(), projectExprs,
      projectNames, null);
    ProjectPrel rightDataFileListInputProject = ProjectPrel.create(
      dataFileFilterList.getCluster(),
      dataFileFilterList.getTraitSet(),
      dataFileFilterList,
      projectExprs,
      projectRowType);

    // build side of join
    // - use broadcast join
    // - we don't have the row count info of unique DMLed files (i.e., after Agg).
    // - so we can't make the choice between BroadcastExchange vs HashToRandomExchange
    BroadcastExchangePrel broadcastExchange = new BroadcastExchangePrel(rightDataFileListInputProject.getCluster(),
      rightDataFileListInputProject.getTraitSet(), rightDataFileListInputProject);

    // manifestScanTF INNER JOIN dataFileListInput
    // ON manifestScanTF.datafilePath == dataFileListInput.__FilePath
    RelDataTypeField pathField = inputDataFiles.getRowType().getField(SystemSchemas.DATAFILE_PATH, false, false);
    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(pathField.getType(), pathField.getIndex()),
      rexBuilder.makeInputRef(broadcastExchange.getRowType().getFieldList().get(0).getType(),
        inputDataFiles.getRowType().getFieldCount())
    );

    // join to restrict data file scan to data files present in dataFileListInput
    // do we need to remove the extra __FilePath col after this? just ignore for now
    return HashJoinPrel.create(
      inputDataFiles.getCluster(),
      inputDataFiles.getTraitSet(),
      inputDataFiles,
      broadcastExchange,
      joinCondition,
      null,
      JoinRelType.INNER);
  }

  /**
   * Builds Manifest and File Scan for positional delete files
   * Consuming operation: Optimize with positional deletes - Delete File scan is needed to determine which data files
   * have positional deletes linked to them and subsequently, need to be rewritten.
   *
   * DataFileScan
   *   |
   *   |
   * exchange on split identity
   *   |
   *   |
   * SplitGen
   *   |
   *   |
   * ManifestScan(DELETE)
   *   |
   *   |
   * Exchange on split identity
   *   |
   *   |
   * ManifestListScan(DELETE)
   */
  public Prel buildDeleteFileScan(OptimizerRulesContext context) {
    DelegatingTableMetadata deleteFileTableMetadata = new DelegatingTableMetadata(icebergScanPrel.getTableMetadata()) {
      @Override
      public BatchSchema getSchema() {
        return ICEBERG_POS_DELETE_FILE_SCHEMA;
      }
    };
    IcebergScanPrel deleteFileScanPrel = new IcebergScanPrel(
      icebergScanPrel.getCluster(),
      icebergScanPrel.getTraitSet(),
      icebergScanPrel.getTable(),
      deleteFileTableMetadata.getStoragePluginId(),
      deleteFileTableMetadata,
      ICEBERG_POS_DELETE_FILE_SCHEMA.getFields().stream().map(i -> SchemaPath.getSimplePath(i.getName())).collect(Collectors.toList()),
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
      NO_SNAPSHOT_DIFF
    );

    ManifestScanOptions deleteManifestScanOptions = new ImmutableManifestScanOptions.Builder()
      .setIncludesSplitGen(false)
      .setManifestContentType(ManifestContentType.DELETES)
      .setIncludesIcebergMetadata(true)
      .build();
    RelNode manifestDeleteScan = icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(), deleteManifestScanOptions);
    manifestDeleteScan = new IcebergSplitGenPrel(manifestDeleteScan.getCluster(), manifestDeleteScan.getTraitSet(), icebergScanPrel.getTable(), manifestDeleteScan,
      deleteFileTableMetadata, SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA, icebergScanPrel.isConvertedIcebergDataset());
    return deleteFileScanPrel.buildDataFileScanWithImplicitPartitionCols(manifestDeleteScan, ImmutableList.of(IMPLICIT_SEQUENCE_NUMBER));
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
    final ScanStats deleteStats = icebergScanPrel.getTableMetadata().getDatasetConfig().getPhysicalDataset()
      .getIcebergMetadata().getDeleteStats();
    return deleteStats != null ? deleteStats.getRecordCount() : 0;
  }

  protected long getDeleteManifestRecordCount() {
    final ScanStats deleteManifestStats = icebergScanPrel.getTableMetadata().getDatasetConfig().getPhysicalDataset()
      .getIcebergMetadata().getDeleteManifestStats();
    return deleteManifestStats != null ? deleteManifestStats.getRecordCount() : 0;
  }

  public boolean hasDeleteFiles() {
    return getDeleteRecordCount() > 0;
  }
  protected IcebergScanPrel getIcebergScanPrel() {return icebergScanPrel;}
}
