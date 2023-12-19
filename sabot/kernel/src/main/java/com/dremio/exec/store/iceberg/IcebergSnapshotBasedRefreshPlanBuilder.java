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

import static com.dremio.exec.planner.common.ScanRelBase.getRowTypeFromProjectedColumns;
import static com.dremio.exec.planner.physical.PrelUtil.addLimitPrel;
import static com.dremio.exec.store.SystemSchemas.ALL_FIELDS_BATCH_SCHEMA;
import static com.dremio.exec.store.dfs.FileSystemRulesFactory.IcebergMetadataFilesystemScanPrule.getInternalIcebergTableMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.iceberg.PartitionSpec;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.IncrementalRefreshByPartitionPlaceholderPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * A class for generating Snapshot Based Incremental Refresh Iceberg plans
 * Both Incremental Refresh Append Only and Incremental Refresh by Partition are supported by this class
 */
public final class IcebergSnapshotBasedRefreshPlanBuilder extends IcebergScanPlanBuilder{
  public IcebergSnapshotBasedRefreshPlanBuilder(final IcebergScanPlanBuilder icebergScanPlanBuilder) {
    super(icebergScanPlanBuilder);
  }

  /**
   * This functions filters an incoming Iceberg scan to only look at new files added between pairs of intervals
   * Each pair has begin snapshot S-old and end snapshot S-new
   * The prerequisite code has already checked that there were no deleted files between S-old an S-new
   * and that the pairs are not overlapping with other pairs
   *         |
   *     DataFileScan
   *         |
   *         |
   * exchange on split identity
   *         |
   *         |
   *     SplitGen
   *         |
   *         |
   *   buildSnapshotDiffFilterPlanAppendOnlyUnionIntervals
   */
  public RelNode buildSnapshotDiffFilterPlanAppendOnlyOptimized() {
    //build union of intervals if needed, otherwise it is just a single snapshot interval scan
    RelNode output = buildSnapshotDiffFilterPlanAppendOnlyUnionIntervals();

    //build split gen, exchange on split identity, DataFileScan
    output = buildDataScanWithSplitGen(output);

    //handle the case where the beginning snapshot is exactly the same as the ending snapshot
    //we add a Limit of 0, which is later optimized to empty plan
    final SnapshotDiffContext snapshotDiffContext = getIcebergScanPrel().getSnapshotDiffContext();
    if(snapshotDiffContext.isSameSnapshot()){
      return generateEmptyPlan(output);
    }

    return output;
  }

  private RelNode generateEmptyPlan(RelNode result) {
    if(result instanceof Prel) {
      return addLimitPrel((Prel) result, 0);
    }
    return result;
  }

  /**
   * Traverse all intervals in snapshotDiffContext.getIntervals() and for each interval
   * we call buildSnapshotDiffFilterPlanAppendOnlySingleInterval to generate a plan
   * scanning the data appended between the beginning and the ending of the interval
   * Then we union the data among all the intervals
   * If there is exactly one interval, no union is needed
   * to buildSnapshotDiffFilterPlanAppendOnlyOptimized
   *      |
   *      |
   * UnionAllPrel (optional, only added if 2 or more inputs)
   *      |
   * (1 or more) from
   *  buildSnapshotDiffFilterPlanAppendOnlySingleInterval
   */
  public RelNode buildSnapshotDiffFilterPlanAppendOnlyUnionIntervals(){
    final SnapshotDiffContext snapshotDiffContext = getIcebergScanPrel().getSnapshotDiffContext();
    final List<SnapshotDiffContext.SnapshotDiffSingleInterval> intervals = snapshotDiffContext.getIntervals();
    if(intervals.size() == 1) {
      //no need to add a Union if there is only one interval
      return buildSnapshotDiffFilterPlanAppendOnlySingleInterval(intervals.get(0));
    } else{
      final List<Prel> args = new ArrayList<>();
      for(final SnapshotDiffContext.SnapshotDiffSingleInterval singleInterval: intervals){
        args.add(buildSnapshotDiffFilterPlanAppendOnlySingleInterval(singleInterval));
      }

      PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getIcebergScanPrel().getCluster());

      Prel result = args.get(0);
      try {
        for (int i = 1; i < args.size(); i++) {
          result = new UnionAllPrel(getIcebergScanPrel().getCluster(), getIcebergScanPrel().getTraitSet(),
            PrelUtil.convertUnionAllInputs(plannerSettings, result, args.get(i)),
            false);
        }
      } catch (final InvalidRelException e) { //this should never happen as we passed in false for checkCompatibility
        throw new RuntimeException("Incompatible Arguments provided for Union Rel" + e);
      }
      return result;
    }
  }

  /**
   * Build an Iceberg plan to scan newly added files between the beginning and ending of the passed in interval
   * The prerequisite is that all operations between beginning and ending of the interval are append only
   * to buildSnapshotDiffFilterPlanAppendOnlyUnionIntervals
   *         |
   *         |
   * Filter(datafile_path_Old isNUll)
   *         |
   *         |
   * HashJoin (LEFT)---------------------------------------|
   * on  datafile_path                                     |
   *         |                                             |
   *         |                                             |
   *         |                                             |
   * ManifestScan(DATA) no split gen              ManifestScan(DATA) no split gen
   *         |                                             |
   *         |                                             |
   * Exchange on split identity                 Exchange on split identity
   *         |                                             |
   *         |                                             |
   * ManifestListScan(DATA, S-new)             ManifestListScan(DATA, S-old)
   *
   *@param interval specifies the beginning and engind snapshot for this plan
   *@return a plan to scan all data appended in S-new since S-old
   */
  public Prel buildSnapshotDiffFilterPlanAppendOnlySingleInterval(final SnapshotDiffContext.SnapshotDiffSingleInterval interval) {
    final SnapshotDiffContext snapshotDiffContext = getIcebergScanPrel().getSnapshotDiffContext();
    TableMetadata endingMetadata = fixMetadataForUnlimitedSplit(interval.getEndingTableMetadata());
    TableMetadata beginningMetadata = fixMetadataForUnlimitedSplit(interval.getBeginningTableMetadata());
    //build the scan on the new Snapshot
    final ImmutableManifestScanOptions scanOptionsNewSnapshot = new ImmutableManifestScanOptions.Builder().setManifestContentType(ManifestContentType.DATA)
      .setIncludesSplitGen(false).setTableMetadata(endingMetadata).build();
    final RelNode newSnapshot = getIcebergScanPrel().buildManifestScan(getDeleteManifestRecordCount(),scanOptionsNewSnapshot);

    //build the scan on the old Snapshot
    final ImmutableManifestScanOptions scanOptionsOldSnapshot = new ImmutableManifestScanOptions.Builder().setManifestContentType(ManifestContentType.DATA)
      .setIncludesSplitGen(false).setTableMetadata(beginningMetadata).build();
    final RelNode oldSnapshot = getIcebergScanPrel().buildManifestScan(getDeleteManifestRecordCount(),scanOptionsOldSnapshot);

    //build the Left join, preserving all the files from the new snapshot
    final RelNode output = buildParameterizedHashJoin(newSnapshot, oldSnapshot,
      getJoinFieldsForSnapshotDiff(), getJoinTypeForSnapshotDiff(snapshotDiffContext));

    //build the filter to remove any files that are common between the old and the new snapshots
    return buildRemoveCommonFiles(output, newSnapshot, oldSnapshot, snapshotDiffContext);

  }

  /** We need special table metadata to query Unlimited Split datasets
   * We will check if the original table metadata in getIcebergScanPrel() is InternalIcebergScanTableMetadata
   * If it is, we will transform the input table metadata into InternalIcebergScanTableMetadata too
   * @param tableMetadata input table metadata
   * @return the transformed metadata if needed, or the original metadata otherwise
   */
  private TableMetadata fixMetadataForUnlimitedSplit(TableMetadata tableMetadata) {
    if(getIcebergScanPrel().getTableMetadata() instanceof InternalIcebergScanTableMetadata) {
      return getInternalIcebergTableMetadata(tableMetadata, getIcebergScanPrel().getContext());
    }
    return tableMetadata;
  }

  /**
   * Join left and right on joinOnFiles with join type specified by joinRelType
   * Prerequisite is that joinOnFields are available both in left and right
   * If there is more than one field in joinOnFields, use AND operator to connect
   * @param left left side of the join
   * @param right right side of the join
   * @param joinOnFields the columns to join on
   * @param joinRelType join type to use
   * @return HashJoinPrel to join left and right on joinOnFiles with join type specified by joinRelType
   */
  private RelNode buildParameterizedHashJoin(final RelNode left, final RelNode right, final List<Field> joinOnFields, final JoinRelType joinRelType) {
    final RelOptCluster cluster = getIcebergScanPrel().getCluster();
    final Map<RelDataTypeField, RelDataTypeField> relDataTypeFieldMapping = new HashMap<>();

    for (final Field field : joinOnFields) {
      relDataTypeFieldMapping.put(getField(left, field.getName()), getField(right, field.getName()));
    }
    final int probeFieldCount = left.getRowType().getFieldCount();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    //build the join condition, equal all the matching fields
    final List<RexNode> inputRels = Lists.newArrayList();
    for (final HashMap.Entry<RelDataTypeField, RelDataTypeField> relDataTypeFieldMapPair : relDataTypeFieldMapping.entrySet()) {
      final RexNode joinConditionSingle = rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(relDataTypeFieldMapPair.getKey().getType(), relDataTypeFieldMapPair.getKey().getIndex()),
        rexBuilder.makeInputRef(relDataTypeFieldMapPair.getValue().getType(),
          probeFieldCount + relDataTypeFieldMapPair.getValue().getIndex()));
      inputRels.add(joinConditionSingle);
    }
    // build the join condition, add AND node on top if needed
    //if we are joining on exactly one equality condition, no AND node on top is needed
    final RexNode joinCondition;
    if (inputRels.size() == 1) {
      joinCondition = inputRels.get(0);
    } else {
      joinCondition = rexBuilder.makeCall(
        SqlStdOperatorTable.AND, inputRels);
    }
    return HashJoinPrel.create(cluster, left.getTraitSet(), left, right,
      joinCondition, null, joinRelType);
  }

  /**
   * Build a scan plan for incremental refresh by partition
   * We will use the SnapshotDiffContext passed in to read the 2 Snapshots involved S-new and S-old
   * We will create a plan to get all the files in S-new and S-old
   * We will join those files to find a list of all files, present in S-new or S-old or both
   * We will filter the list of files to only the list of files in S-new or S-old, but not both
   * That is the list of modified files between those 2 snapshots
   * We will create a table function of type ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY
   * The table function will raise the modified files to the partitions modified
   * Then we do HashAgg to only select each partition once
   * We use the result of the HashAgg above to filter the files to be used for the data scan
   * The data scan goes through function ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY to raise to partition filter
   *Then we join both sides using inner join on ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY output
   * Finally we have the SplitGen, exchange on split identity, DataFileScan which are standard
   *       |
   *     DataFileScan
   *       |
   *       |
   * exchange on split identity
   *       |
   *       |
   *    SplitGen
   *       |
   *       |
   * HashJoin(INNER) on datafile_path-----------------------------------------
   *       |                                                                  |
   *       |                                                                  |
   * HashAgg to select distinct partitions to keep                            |
   *      |                                                                   |
   *      |                                                                   |
   * Table Function ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY                      |
   *      |                                                            Table Function
   *      |                                                  ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY
   * Project(partition info from new and old)                                 |
   *      |                                                                   |
   *      |                                                       ManifestScan(ALL) no split gen
   * Filter(datafile_path_Old isNUll or datafile_path_New isNUll)             |
   *      |                                                        Exchange on split identity
   *      |                                                                   |
   * HashJoin --------------------------------------------------|  ManifestListScan(ALL, S-new)
   * on partitionKey, datafile_path OUTER                       |
   *      |                                                     |
   *      |                                                     |
   * ManifestScan(ALL) no split gen                       ManifestScan(ALL) no split gen
   *      |                                                     |
   *      |                                                     |
   * Exchange on split identity                            Exchange on split identity
   *      |                                                     |
   *      |                                                     |
   * ManifestListScan(ALL, S-new)                      ManifestListScan(ALL, S-old)
   */
  public RelNode buildSnapshotDiffPlanFilterPartitions() {
    final SnapshotDiffContext snapshotDiffContext = getIcebergScanPrel().getSnapshotDiffContext();
    RelNode input;
    //build the input main scan
    //if it has deletes, then the standard delete plan is generated by buildManifestScanPlanWithDeletes
    //otherwise we build a simple plan using the icebergScanPrel
    //either way we make sure that the IncludesIcebergPartitionInfo is set to true
    if(hasDeleteFiles()){
      input = buildManifestScanPlanWithDeletes(true);
    } else{
      final ManifestScanOptions manifestScanOptions = new ImmutableManifestScanOptions.Builder()
        .setIncludesSplitGen(false)
        .setManifestContentType(ManifestContentType.DATA)
        .setIncludesIcebergPartitionInfo(true)
        .build();
      input = getIcebergScanPrel().buildManifestScan(getIcebergScanPrel().getSurvivingFileCount(), manifestScanOptions);
    }

    //build a plan to compare new and old snapshot and find any files in the delta
    //that means any file in old and not new, plus any files in new and not old
    //files that are in both new and old snapshot are filtered out as those are not modified
    final ImmutableManifestScanOptions scanOptionsNewSnapshot = new ImmutableManifestScanOptions.Builder().setManifestContentType(ManifestContentType.DATA)
      .setIncludesSplitGen(false)
      .setTableMetadata(fixMetadataForUnlimitedSplit(snapshotDiffContext.getIntervals().get(0).getEndingTableMetadata()))
      .setManifestContentType(ManifestContentType.ALL)
      .setIncludesIcebergPartitionInfo(true)
      .build();
    final RelNode newSnapshotScan = getIcebergScanPrel().buildManifestScan(getDataManifestRecordCount() + getDeleteManifestRecordCount(),scanOptionsNewSnapshot);

    final ImmutableManifestScanOptions scanOptionsOldSnapshot = new ImmutableManifestScanOptions.Builder().setManifestContentType(ManifestContentType.DATA)
      .setIncludesSplitGen(false)
      .setTableMetadata(fixMetadataForUnlimitedSplit(snapshotDiffContext.getIntervals().get(0).getBeginningTableMetadata()))
      .setManifestContentType(ManifestContentType.ALL)
      .setIncludesIcebergPartitionInfo(true)
      .build();
    final RelNode oldSnapshotScan = getIcebergScanPrel().buildManifestScan(getDataManifestRecordCount() + getDeleteManifestRecordCount(),scanOptionsOldSnapshot);

    //outer join old snapshot files and new snapshot files on DATAFILE_PATH
    RelNode output = buildParameterizedHashJoin(newSnapshotScan, oldSnapshotScan,
      getJoinFieldsForSnapshotDiff(), getJoinTypeForSnapshotDiff(snapshotDiffContext));

    //filter out all the files that are in both newSnapshotScan and oldSnapshotScan
    //if a file is in both snapshots it has not been modified, so it can be removed
    output = buildRemoveCommonFiles(output, newSnapshotScan, oldSnapshotScan, snapshotDiffContext);

    //project partition info from newSnapshotScan and oldSnapshotScan, so that the columns align
    output = buildCaseStatementProjectBeforeMainJoin(output, newSnapshotScan, oldSnapshotScan,
      getCaseStatementAndHashFieldsBeforeMainJoin(snapshotDiffContext));

    //raise the partition to the spec provided in snapshotDiffContext.getBaseDatasetTargetPartitionSpec()
    output = buildIncrementalRefreshJoinKeyTableFunction(output,snapshotDiffContext.getBaseDatasetTargetPartitionSpec());

    //make the join key unique. If we don't, we will get double counting
    //We don't want the inner join to filter the data files to be an expanding join
    //Each data file should be included exactly 0 or 1 times
    output = buildParameterizedHashAgg(output, getJoinFieldsForBaseDatasetFiltering(snapshotDiffContext));

    //we will reuse this part of the plan to delete the affected partitions from the reflection
    //save this part of the plan in snapshotDiffContext
    snapshotDiffContext.setDeleteFilesFilter(output);

    //raise the main scan partition to the spec provided in snapshotDiffContext.getBaseDatasetTargetPartitionSpec()
    //we need to raise both left and right part of the join, so we join on the same thing
    //Notice that we can't do distinct on this part of the plan,
    //because we want to keep all the files selected
    //Even if there are multiple files selected for the same partition, we still need to get the data from all of them
    input = buildIncrementalRefreshJoinKeyTableFunction(input,snapshotDiffContext.getBaseDatasetTargetPartitionSpec());

    //main join to filter the selected files
    output = buildParameterizedHashJoin(input, output, getJoinFieldsForBaseDatasetFiltering(snapshotDiffContext), JoinRelType.INNER);

    //all the extra files have been already filtered out by the plan above
    //this part of the plan is standard, just do Split Gen and Data file scan on the remaining files
    output = buildSplitGen(output);
    output = getIcebergScanPrel().buildDataFileScan(output);

    return output;
  }

  /**
   * Builds an Incremental Refresh Join Key Table function using input and the target partition spec
   * @param input input node for this table function
   * @param partitionSpec target partition spec
   * @return Prel that is actually Incremental Refresh Join Key Table function on top of input
   */
  private RelNode buildIncrementalRefreshJoinKeyTableFunction(final RelNode input, final PartitionSpec partitionSpec) {

    final List<SchemaPath> outputColumns = new ArrayList<>();
    final BatchSchema outputSchema = getIncrementalRefreshJoinKeySchema(outputColumns, input);
    final RelDataType outputRowType = getRowTypeFromProjectedColumns(outputColumns,outputSchema,input.getCluster());
    final ByteString partitionSpecByteString = ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(partitionSpec));
    final String schemaAsJson = IcebergSerDe.serializedSchemaAsJson(partitionSpec.schema());
    final TableFunctionConfig icebergTransformTableFunctionConfig = TableFunctionUtil.
      getIcebergIncrementalRefreshJoinKeyTableFunctionConfig(partitionSpecByteString,schemaAsJson, outputSchema, outputColumns, getIcebergScanPrel().getTableMetadata());

    return new IcebergIncrementalRefreshJoinKeyPrel(
      input.getCluster(),
      input.getTraitSet(),
      input.getTable(),
      input,
      null,
      icebergTransformTableFunctionConfig,
      outputRowType,
      rm -> rm.getRowCount(input),
      null,
      ImmutableList.of(),
      getIcebergScanPrel().getTableMetadata().getUser());
  }

  /**
   * A parameterized function to project the same columns coming from two different scans
   * For each row, almost half the columns are empty, and we need to get the correct columns for each row
   * For each field in joinFieldsForBaseDatasetFiltering, we will project CASE newDataFilePath is NULL oldField ELSE newField
   * @param input  the input prel (containing both fields from newSnapshot and oldSnapshot)
   * @param newSnapshot fields from the new snapshot
   * @param oldSnapshot fields from the old snapshot
   * @param joinFieldsForBaseDatasetFiltering fields that we need to keep (that are both in old and new snapshot)
   * @return a project that only keeps joinFieldsForBaseDatasetFiltering.size() columns, all populated with values from either the old or the new snapshot
   */
  private RelNode buildCaseStatementProjectBeforeMainJoin(final RelNode input,
                                                          final RelNode newSnapshot,
                                                          final RelNode oldSnapshot,
                                                          final List<Field> joinFieldsForBaseDatasetFiltering) {
    final RelOptCluster cluster = getIcebergScanPrel().getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    final List<RexNode> expressions = Lists.newArrayList();
    final List<String> names = Lists.newArrayList();
    final int probeFieldCount = newSnapshot.getRowType().getFieldCount();

    final RelDataTypeField newDataFilePath = getField(newSnapshot, SystemSchemas.DATAFILE_PATH);
    for (final Field field : joinFieldsForBaseDatasetFiltering) {
      //for each field add
      //CASE newDataFilePath is NULL oldField ELSE newField
      //for example CASE newDataFilePath is NULL, oldPartitionID else newPartitionID

      final RelDataTypeField newField = getField(newSnapshot, field.getName());
      final RelDataTypeField oldField = getField(oldSnapshot, field.getName());

      final RexNode projectExpression = rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
          rexBuilder.makeInputRef(newDataFilePath.getType(), newDataFilePath.getIndex())),
        rexBuilder.makeInputRef(oldField.getType(), probeFieldCount + oldField.getIndex()),
        rexBuilder.makeInputRef(newField.getType(), newField.getIndex()));


      expressions.add(projectExpression);
      names.add(field.getName());
    }

    final RelDataType projectType = RexUtil.createStructType(cluster.getTypeFactory(), expressions, names, null);
    return ProjectPrel.create(cluster, input.getTraitSet(), input, expressions, projectType);
  }

  /**
   * Given input and a list of fields to aggregate on build a HashAggPrel
   * @param input input rel to agg on top of
   * @param groupByLevel fields this HashAgg will group by
   * @return HashAggPrel to aggregate input to groupByLevel level
   */
  private RelNode buildParameterizedHashAgg(final RelNode input, final List<Field> groupByLevel) {

    final RelOptCluster cluster = getIcebergScanPrel().getCluster();

    final List<Integer> groupingFields = new ArrayList<>();

    for (final Field field : groupByLevel) {
      final RelDataTypeField filePathField = input.getRowType().getField(field.getName(), false, false);
      groupingFields.add(filePathField.getIndex());
    }

    final ImmutableBitSet groupSet = ImmutableBitSet.of(groupingFields);

    try {
      return HashAggPrel.create(cluster, input.getTraitSet(), input, groupSet, ImmutableList.of(groupSet),
        ImmutableList.of(), null);
    } catch (final InvalidRelException e) {
      throw new RuntimeException("Failed to create HashAggPrel during Reflection Incremental Refresh .");
    }
  }

  /**
   * Removes the files not needed based on the FilterApplyOptions
   * For FILTER_DATA_FILES, it will only keep newly added files
   * For FILTER_PARTITIONS, it will keep newly added files and newly deleted files
   * @param inputNode the output of a join operation
   *                  containing all the fields from the plan for the newSnapshot and the plan for the oldSnapshot
   * @param newSnapshot plan for the newSnapshot
   * @param oldSnapshot plan for the oldSnapshot
   * @param snapshotDiffContext contains Filter Options
   * @return a plan that filters out inputNode to only contain the files needed depending on SnapshotDiffContext.FilterApplyOptions
   */
  private Prel buildRemoveCommonFiles(final RelNode inputNode, final RelNode newSnapshot, final RelNode oldSnapshot, final SnapshotDiffContext snapshotDiffContext) {
    final RelOptCluster cluster = getIcebergScanPrel().getCluster();

    final RexBuilder rexBuilder = cluster.getRexBuilder();


    final int probeFieldCount = newSnapshot.getRowType().getFieldCount();
    final RexNode filterCondition;
    final RelDataTypeField oldDataFilePath = getField(oldSnapshot, SystemSchemas.DATAFILE_PATH);
    if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES) {
      //we want to keep only newly added files, so the filter should be
      //oldSnapshot.DATAFILE_PATH IS_NULL
      filterCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
        rexBuilder.makeInputRef(oldDataFilePath.getType(), probeFieldCount + oldDataFilePath.getIndex()));
    } else if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
      //we want to keep only files that are in the new snapshot or the old snapshot, but not in both
      //so we only pick files that were due to the previous outer join
      //That means the path in Old Snapshot or New Snapshot is null
      //newSnapshot.DATAFILE_PATH IS_NULL OR oldSnapshot.DATAFILE_PATH IS_NULL

      final RelDataTypeField newDataFilePath = getField(newSnapshot, SystemSchemas.DATAFILE_PATH);

      filterCondition = rexBuilder.makeCall(
        SqlStdOperatorTable.OR,
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
          rexBuilder.makeInputRef(oldDataFilePath.getType(), newDataFilePath.getIndex())),
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
          rexBuilder.makeInputRef(oldDataFilePath.getType(), probeFieldCount + oldDataFilePath.getIndex())));
    } else{
      throw UserException.validationError()
        .message("Invalid FilterApplyOptions in buildRemoveCommonFiles.")
        .buildSilently();
    }

    return FilterPrel.create(
      inputNode.getCluster(),
      inputNode.getTraitSet(),
      inputNode,
      filterCondition);

  }

  /**
   * Build a filter to limit files to be deleted from the reflection to only those
   * in modified partitions of the base dataset between the S-old and S-new snapshots
   * As a part of the plan is not ready yet we insert a IncrementalRefreshByPartitionPlaceholderPrel, that is later replaced by the actual plan
   * @param input scan of all partitions of the reflection
   * @param snapshotDiffContext context, that has the old and the new snapshots
   * @return a filtered list of files belonging to the reflection to delete
   * The files belong to modified partitions of the base dataset, which we will recalculate and read.
   */
  public RelNode buildIncrementalReflectionDeleteFilesFilter(RelNode input, final SnapshotDiffContext snapshotDiffContext) {
    final List<SchemaPath> manifestListReaderColumns = new ArrayList<>();
    final BatchSchema manifestListReaderSchema = getPlaceholderPrelSchema(manifestListReaderColumns);
    final RelDataType rowType = getRowTypeFromProjectedColumns(manifestListReaderColumns,manifestListReaderSchema,input.getCluster());
    final Prel readerChild = new IncrementalRefreshByPartitionPlaceholderPrel(input.getCluster(),
      input.getTraitSet(),
      rowType,
      snapshotDiffContext);

    input = buildIncrementalRefreshJoinKeyTableFunction(input,snapshotDiffContext.getReflectionPartitionSpec());

    return buildParameterizedHashJoin(input, readerChild, getJoinFieldsForBaseDatasetFiltering(snapshotDiffContext), JoinRelType.INNER);
  }

  /**
   * Builds a BatchSchema for the IncrementalRefreshByPartitionPlaceholderPrel
   * @param manifestFileReaderColumns list of columns in the schema
   * @return a BatchSchema for the IncrementalRefreshByPartitionPlaceholderPrel
   */
  private BatchSchema getPlaceholderPrelSchema(final List<SchemaPath> manifestFileReaderColumns) {
    final BatchSchema manifestFileReaderSchema = BatchSchema.newBuilder()
      .addField(Field.nullable(SystemSchemas.INCREMENTAL_REFRESH_JOIN_KEY, Types.MinorType.VARCHAR.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
      .build();

    manifestFileReaderSchema.getFields().forEach(
      f -> manifestFileReaderColumns.add(SchemaPath.getSimplePath(f.getName())));

    return manifestFileReaderSchema;
  }

  /**
   * Builds a customized BatchSchema including all the fields in input plus INCREMENTAL_REFRESH_JOIN_KEY
   * IncrementalRefreshJoinKey table function is used in multiple places, so it has different input schema each time
   * We use the ALL_FIELDS_BATCH_SCHEMA to find the correct field for each field name in the input,
   * and we add one more field called INCREMENTAL_REFRESH_JOIN_KEY
   * @param manifestFileReaderColumns columns to add to
   * @param input child RelNode to read the field names from
   * @return BatchSchema corresponding to all the fields in input plus INCREMENTAL_REFRESH_JOIN_KEY
   */
  private BatchSchema getIncrementalRefreshJoinKeySchema(final List<SchemaPath> manifestFileReaderColumns, final RelNode input) {

    final SchemaBuilder schemaBuilder = BatchSchema.newBuilder()
      .addField(Field.nullable(SystemSchemas.INCREMENTAL_REFRESH_JOIN_KEY, Types.MinorType.VARCHAR.getType()))
      .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
    for(final RelDataTypeField relDataTypeField:input.getRowType().getFieldList()){
      final Field field = ALL_FIELDS_BATCH_SCHEMA.findField(relDataTypeField.getName());
      if(field == null){
        throw new RuntimeException("Could not find datatype for field " + relDataTypeField.getName());
      }
      schemaBuilder.addField(field);
    }


    final BatchSchema incrementalRefreshJoinKeySchema = schemaBuilder.build();
    incrementalRefreshJoinKeySchema.getFields().forEach(
      f -> manifestFileReaderColumns.add(SchemaPath.getSimplePath(f.getName())));

    return incrementalRefreshJoinKeySchema;
  }

  /**
   * Builds a list of fields to join on when we perform the main filtering join
   * for Incremental Refresh by Partition or Append only
   * @return a list of fields representing the join on column
   */
  public List<Field> getJoinFieldsForSnapshotDiff() {
    final List<Field> fieldsToAdd = Lists.newArrayList();
    fieldsToAdd.add(Field.nullable(SystemSchemas.DATAFILE_PATH, Types.MinorType.VARCHAR.getType()));
    return fieldsToAdd;
  }

  /**
   * Gets the join type to use when we perform the main filtering join
   * for Incremental Refresh by Partition or Append only
   * Join type depends on the FilterApplyOptions inside the snapshotDiffContext
   * @param snapshotDiffContext Diff Context to use
   * @return Join type to use
   */
  public JoinRelType getJoinTypeForSnapshotDiff(final SnapshotDiffContext snapshotDiffContext) {
    if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES) {
      return JoinRelType.LEFT;
    } else if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
      return JoinRelType.FULL;
    }
    return JoinRelType.FULL;
  }

  /**
   * Builds a list of fields to use for the case statement and hash join before the main join
   * for Incremental Refresh by Partition or Append only
   * @param snapshotDiffContext Diff Context to use
   * @return list of fields we use in the case statement
   */
  public List<Field> getCaseStatementAndHashFieldsBeforeMainJoin(final SnapshotDiffContext snapshotDiffContext){
    final List<Field> fieldsToAdd = Lists.newArrayList();
    if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES) {
      fieldsToAdd.add(Field.nullable(SystemSchemas.DATAFILE_PATH, Types.MinorType.VARCHAR.getType()));
    } else if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
      fieldsToAdd.add(Field.nullable(SystemSchemas.PARTITION_SPEC_ID, Types.MinorType.INT.getType()));
      fieldsToAdd.add(Field.nullable(SystemSchemas.PARTITION_KEY, Types.MinorType.VARBINARY.getType()));
      fieldsToAdd.add(Field.nullable(SystemSchemas.PARTITION_INFO, Types.MinorType.VARBINARY.getType()));
    }
    return fieldsToAdd;
  }

  /**
   * Builds a list of fields to filter on when we filter base scan for
   * for Incremental Refresh by Partition or Append only
   * The exact list of fields depends on the FilterApplyOptions in snapshotDiffContext
   * @param snapshotDiffContext Diff Context to use
   * @return list of fields to filter on
   */
  public List<Field> getJoinFieldsForBaseDatasetFiltering(final SnapshotDiffContext snapshotDiffContext) {
    final List<Field> fieldsToAdd = Lists.newArrayList();
    if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_DATA_FILES) {
      fieldsToAdd.add(Field.nullable(SystemSchemas.DATAFILE_PATH, Types.MinorType.VARCHAR.getType()));
    } else if (snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.FILTER_PARTITIONS) {
      fieldsToAdd.add(Field.nullable(SystemSchemas.INCREMENTAL_REFRESH_JOIN_KEY, Types.MinorType.VARCHAR.getType()));
    }
    return fieldsToAdd;
  }
}
