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

import java.util.List;

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
import org.apache.iceberg.ManifestContent;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.google.common.collect.ImmutableList;

public class IcebergScanPlanBuilder {

  private final IcebergScanPrel icebergScanPrel;

  public IcebergScanPlanBuilder(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    RelOptTable table,
    TableMetadata tableMetadata,
    List<SchemaPath> projectedColumns,
    OptimizerRulesContext context,
    ManifestScanFilters manifestScanFilters) {
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
      false,
      null,
      context,
      false,
      null,
      null,
      false,
      manifestScanFilters);
  }

  private IcebergScanPlanBuilder(
      IcebergScanPrel icebergScanPrel) {
    this.icebergScanPrel = icebergScanPrel;
  }

  public static IcebergScanPlanBuilder fromDrel(ScanRelBase drel, OptimizerRulesContext context,
      boolean isArrowCachingEnabled, boolean canUsePartitionStats) {
    return fromDrel(drel, context, drel.getTableMetadata(), isArrowCachingEnabled, false, canUsePartitionStats);
  }

  public static IcebergScanPlanBuilder fromDrel(ScanRelBase drel, OptimizerRulesContext context,
      TableMetadata tableMetadata, boolean isArrowCachingEnabled, boolean isConvertedIcebergDataset, boolean canUsePartitionStats) {
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
      isArrowCachingEnabled,
      filterableScan.getPartitionFilter(),
      context,
      isConvertedIcebergDataset,
      filterableScan.getSurvivingRowCount(),
      filterableScan.getSurvivingFileCount(),
      canUsePartitionStats,
      ManifestScanFilters.empty());
    return new IcebergScanPlanBuilder(prel);
  }

  /**
   * This builds scan plans both with and without delete files.
   *
   * Without delete files, it simply returns an IcebergScanPrel.  See IcebergScanPrel.finalizeRel() for details
   * on the expansion.
   *
   * With delete files, the generated plan is as follows (details for pruning/filtering omitted for brevity):
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
  public RelNode build() {
    RelNode output;
    if (hasDeleteFiles()) {
      RelNode data = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
        new ImmutableManifestScanOptions.Builder().setManifestContent(ManifestContent.DATA).build());
      RelNode deletes = icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(),
        new ImmutableManifestScanOptions.Builder().setManifestContent(ManifestContent.DELETES).build());

      output = buildDataAndDeleteFileJoinAndAggregate(data, deletes);
      output = buildSplitGen(output);
      output = icebergScanPrel.buildDataFileScan(output);
    } else {
      // no delete files, just return IcebergScanPrel which will get expanded in FinalizeRel stage
      output = icebergScanPrel;
    }

    return output;
  }

  /**
   * This builds manifest scan plans both with and without delete files.
   */
  public RelNode buildManifestRel(ManifestScanOptions manifestScanOptions) {

    RelNode output = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
      new ImmutableManifestScanOptions.Builder().from(manifestScanOptions).setManifestContent(ManifestContent.DATA).build());

    if (hasDeleteFiles()) {
      RelNode deletes = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(),
        new ImmutableManifestScanOptions.Builder().from(manifestScanOptions).setManifestContent(ManifestContent.DELETES).build());
      output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
    }

    return output;
  }

  public RelNode buildWithDmlDataFileFiltering(RelNode dataFileFilterList) {
    ManifestScanOptions manifestScanOptions =  new ImmutableManifestScanOptions.Builder()
      .setManifestContent(ManifestContent.DATA).setIncludesSplitGen(false).build();
    RelNode output = icebergScanPrel.buildManifestScan(getDataManifestRecordCount(), manifestScanOptions);
    // join with the unique data files to filter
    output = buildDataFileFilteringSemiJoin(output, dataFileFilterList);

    if (hasDeleteFiles()) {
      // if the table has delete files, add a delete manifest scan and join/aggregate applicable delete files to
      // data files
      manifestScanOptions = new ImmutableManifestScanOptions.Builder().from(manifestScanOptions)
        .setManifestContent(ManifestContent.DELETES).build();
      RelNode deletes = icebergScanPrel.buildManifestScan(getDeleteManifestRecordCount(), manifestScanOptions);
      output = buildDataAndDeleteFileJoinAndAggregate(output, deletes);
    }

    // perform split gen
    output = buildSplitGen(output);

    return icebergScanPrel.buildDataFileScan(output);
  }

  private RelNode buildSplitGen(RelNode input) {
    BatchSchema splitGenOutputSchema =
        input.getRowType().getField(SystemSchemas.DELETE_FILES, false, false) == null ?
            SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA :
            SystemSchemas.ICEBERG_SPLIT_GEN_WITH_DELETES_SCHEMA;

    // perform split generation
    return new IcebergSplitGenPrel(input.getCluster(), input.getTraitSet(), icebergScanPrel.getTable(), input,
        icebergScanPrel.getTableMetadata(), splitGenOutputSchema, icebergScanPrel.isConvertedIcebergDataset());
  }

  private RelNode buildDataAndDeleteFileJoinAndAggregate(RelNode data, RelNode deletes) {
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
    List<RexNode> exprs = ImmutableList.of(
        rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.DATAFILE_PATH)),
        rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.FILE_SIZE)),
        rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.PARTITION_INFO)),
        rexBuilder.makeInputRef(output, getFieldIndex(data, SystemSchemas.COL_IDS)),
        rexBuilder.makeInputRef(output, data.getRowType().getFieldCount() +
            getFieldIndex(deletes, SystemSchemas.DELETE_FILE)));
    List<String> names = ImmutableList.of(
        SystemSchemas.DATAFILE_PATH,
        SystemSchemas.FILE_SIZE,
        SystemSchemas.PARTITION_INFO,
        SystemSchemas.COL_IDS,
        SystemSchemas.DELETE_FILE);
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
    // - so we cannt make the choice between BroadcastExchange vs HashToRandomExchange
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

  private static RelDataTypeField getField(RelNode rel, String fieldName) {
    return rel.getRowType().getField(fieldName, false, false);
  }

  private static int getFieldIndex(RelNode rel, String fieldName) {
    return rel.getRowType().getField(fieldName, false, false).getIndex();
  }

  private long getDataManifestRecordCount() {
    return icebergScanPrel.getSurvivingFileCount();
  }

  private long getDeleteRecordCount() {
    ScanStats deleteStats = icebergScanPrel.getTableMetadata().getDatasetConfig().getPhysicalDataset()
        .getIcebergMetadata().getDeleteStats();
    return deleteStats != null ? deleteStats.getRecordCount() : 0;
  }

  private long getDeleteManifestRecordCount() {
    ScanStats deleteManifestStats = icebergScanPrel.getTableMetadata().getDatasetConfig().getPhysicalDataset()
        .getIcebergMetadata().getDeleteManifestStats();
    return deleteManifestStats != null ? deleteManifestStats.getRecordCount() : 0;
  }

  private boolean hasDeleteFiles() {
    return getDeleteRecordCount() > 0;
  }
}
