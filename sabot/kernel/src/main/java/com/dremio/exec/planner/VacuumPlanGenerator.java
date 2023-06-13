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
package com.dremio.exec.planner;

import static com.dremio.exec.store.RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_CONTENT;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.RECORDS;
import static com.dremio.exec.store.iceberg.model.IcebergConstants.ADDED_DATA_FILES;
import static com.dremio.exec.store.iceberg.model.IcebergConstants.DELETED_DATA_FILES;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergFileType;
import com.dremio.exec.store.iceberg.IcebergManifestListScanPrel;
import com.dremio.exec.store.iceberg.IcebergManifestScanPrel;
import com.dremio.exec.store.iceberg.IcebergOrphanFileDeletePrel;
import com.dremio.exec.store.iceberg.IcebergSnapshotsPrel;
import com.dremio.exec.store.iceberg.PartitionStatsScanPrel;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.io.file.FileSystem;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/***
 * Expand plans for VACUUM TABLE.
 */
public class VacuumPlanGenerator {
  private static final long ESTIMATED_RECORDS_PER_MANIFEST = 330000;
  private final RelOptTable table;
  private final RelOptCluster cluster;
  private final RelTraitSet traitSet;
  private final TableMetadata tableMetadata;
  private final VacuumOptions vacuumOptions;
  private final CreateTableEntry createTableEntry;
  private Table icebergTable = null;
  private long snapshotsCount = 0L;
  private long dataFileEstimatedCount = 0L;
  private long manifestFileEstimatedCount = 0L;

  public VacuumPlanGenerator(RelOptTable table, RelOptCluster cluster, RelTraitSet traitSet, TableMetadata tableMetadata,
                             CreateTableEntry createTableEntry, VacuumOptions vacuumOptions) {
    this.table = Preconditions.checkNotNull(table);
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.createTableEntry = createTableEntry;
    this.vacuumOptions = Preconditions.checkNotNull(vacuumOptions, "VacuumOption cannot be null.");
    loadIcebergTable();
  }

  /*
   *                            UnionExchangePrel
   *                                     │
   *                                     │
   *                        IcebergOrphanFileDeleteTF
   *                                     │
   *                                     │
   *                       Filter (live.filePath = null)
   *                                     │
   *                                     │
   *              HashJoin (expired.filePath=live.filePath (LEFT))
   *                                  │    │
   *            ┌─────────────────────┘    └──────────┐
   *            │                                     │
   * Project (filepath, filetype)        Project (filepath, filetype)
   *            │                                     │
   *            │                                     │
   *            │                        HashAgg(filepath [deduplicate])
   *            │                                     │
   *            │                                     │
   * Project                             Project
   * [ (filepath,    filetype)          [ (filepath,    filetype)
   *       │             │                    │             │
   * (datafilepath, filecontent) ]      (datafilepath, filecontent) ]
   *            │                                     │
   *            │                                     │
   * IcebergManifestScanTF               IcebergManifestScanTF
   *            │                                     │
   *            │                                     │
   * IcebergManifestListScanTF           IcebergManifestListScanTF
   *            │                                     │
   *            │                                     │
   * PartitionStatsScanTF                PartitionStatsScanTF
   *            │                                     │
   *            │                                     │
   * ExpireSnapshotScan                  ExpiredSnapshotScan
   * (Expired snapshot ids)              (Live snapshot ids)
   */

  public Prel buildPlan() {
    try {
      Prel expiredSnapshotFilesPlan = filePathAndTypeScanPlan(SnapshotsScanOptions.Mode.EXPIRED_SNAPSHOTS);
      Prel liveSnapshotsFilesPlan = deDupFilePathAndTypeScanPlan(SnapshotsScanOptions.Mode.LIVE_SNAPSHOTS);
      Prel orphanFilesPlan = orphanFilesPlan(expiredSnapshotFilesPlan, liveSnapshotsFilesPlan);
      Prel deleteOrphanFilesPlan = deleteOrphanFilesPlan(orphanFilesPlan);
      return outputSummaryPlan(deleteOrphanFilesPlan);
    } catch (InvalidRelException e) {
      throw new RuntimeException(e);
    }
  }

  private Prel orphanFilesPlan(Prel expired, Prel live) {
    Prel joinPlan = joinLiveAndExpiredPlan(expired, live);
    // Need to count left side fields.
    final int leftFieldCount = joinPlan.getInput(0).getRowType().getFieldCount();

    // Orphan file paths: right.FILE_PATH IS NULL
    RelDataTypeField rightRowIndexField = joinPlan.getInput(1).getRowType()
      .getField(FILE_PATH, false, false);

    return addColumnIsNullFilter(joinPlan, rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex());
  }

  private Prel joinLiveAndExpiredPlan(Prel expired, Prel live) {
    RexBuilder rexBuilder = expired.getCluster().getRexBuilder();

    // Left side: Source files from expired snapshots
    DistributionTrait leftDistributionTrait = getHashDistributionTraitForFields(expired.getRowType(), ImmutableList.of(FILE_PATH));
    RelTraitSet leftTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL)
      .plus(leftDistributionTrait);
    HashToRandomExchangePrel leftSourceFilePathHashExchange = new HashToRandomExchangePrel(cluster, leftTraitSet,
      expired, leftDistributionTrait.getFields());

    // Right side: Source files from live snapshots
    DistributionTrait rightDistributionTrait = getHashDistributionTraitForFields(live.getRowType(), ImmutableList.of(FILE_PATH));
    RelTraitSet rightTraitSet = cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL)
      .plus(rightDistributionTrait);
    HashToRandomExchangePrel rightSourceFilePathHashExchange = new HashToRandomExchangePrel(cluster, rightTraitSet,
      live, rightDistributionTrait.getFields());

    // hash join on FILE_PATH == FILE_PATH

    RelDataTypeField leftSourceFilePathField = leftSourceFilePathHashExchange.getRowType()
      .getField(FILE_PATH, false, false);
    RelDataTypeField rightSourceFilePathField = rightSourceFilePathHashExchange.getRowType()
      .getField(FILE_PATH, false, false);

    int leftFieldCount = leftSourceFilePathHashExchange.getRowType().getFieldCount();
    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(leftSourceFilePathField.getType(), leftSourceFilePathField.getIndex()),
      rexBuilder.makeInputRef(rightSourceFilePathField.getType(), leftFieldCount + rightSourceFilePathField.getIndex()));

    return HashJoinPrel.create(
      leftSourceFilePathHashExchange.getCluster(),
      leftSourceFilePathHashExchange.getTraitSet(),
      leftSourceFilePathHashExchange,
      rightSourceFilePathHashExchange,
      joinCondition,
      null,
      JoinRelType.LEFT);
  }

  private Prel deDupFilePathAndTypeScanPlan(SnapshotsScanOptions.Mode scanMode) throws InvalidRelException {
    Prel manifestPlan = filePathAndTypePlanFromManifest(scanMode);
    Prel filePathAndTypeProject = projectDataFileAndType(manifestPlan);
    Prel deDupFilePathPlan = reduceDuplicateFilePaths(filePathAndTypeProject);
    return projectFilePathAndType(deDupFilePathPlan);
  }

  private Prel filePathAndTypeScanPlan(SnapshotsScanOptions.Mode scanMode) throws InvalidRelException {
    Prel manifestPlan = filePathAndTypePlanFromManifest(scanMode);
    Prel filePathAndTypeProject = projectDataFileAndType(manifestPlan);
    return projectFilePathAndType(filePathAndTypeProject);
  }

  private Prel filePathAndTypePlanFromManifest(SnapshotsScanOptions.Mode scanMode) throws InvalidRelException {
    Prel snapshotsScanPlan = snapshotsScanPlan(scanMode);
    Prel partitionStatsScan = getPartitionStatsScanPrel(snapshotsScanPlan);
    Prel manifestListScan = getManifestListScanPrel(partitionStatsScan);
    return getManifestScanPrel(manifestListScan);
  }

  private Prel snapshotsScanPlan(SnapshotsScanOptions.Mode scanMode) {
    SnapshotsScanOptions snapshotsOption = new SnapshotsScanOptions(scanMode, vacuumOptions.getOlderThanInMillis(), vacuumOptions.getRetainLast());
    return new IcebergSnapshotsPrel(
      cluster,
      traitSet,
      tableMetadata,
      createTableEntry.getIcebergTableProps(),
      snapshotsOption,
      snapshotsCount,
      1);
  }

  private Prel getManifestListScanPrel(Prel input) {
    BatchSchema manifestListsReaderSchema = SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    List<SchemaPath> manifestListsReaderColumns = manifestListsReaderSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList());
    return new IcebergManifestListScanPrel(
      input.getCluster(),
      input.getTraitSet(),
      table,
      input,
      tableMetadata,
      manifestListsReaderSchema,
      manifestListsReaderColumns,
      input.getEstimatedSize() + manifestFileEstimatedCount);
  }

  private Prel getPartitionStatsScanPrel(Prel input) {
    BatchSchema partitionStatsScanSchema = ICEBERG_SNAPSHOTS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    // TODO: it could be further improved whether it needs to apply PartitionStatsScan, if table is written by other engines,
    // or the partition stats metadata entry is not present.
    long estimatedRows = 2 * input.getEstimatedSize();
    return new PartitionStatsScanPrel(input.getCluster(), input.getTraitSet(), table, input, partitionStatsScanSchema, tableMetadata, estimatedRows);
  }

  private Prel getManifestScanPrel(Prel input) {
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(input.getCluster(), input.getTraitSet(),
      input, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(tableMetadata, true));

    BatchSchema manifestFileReaderSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    List<SchemaPath> manifestFileReaderColumns = manifestFileReaderSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList());

    return new IcebergManifestScanPrel(manifestSplitsExchange.getCluster(), manifestSplitsExchange.getTraitSet().plus(DistributionTrait.ANY), table,
      manifestSplitsExchange, tableMetadata, manifestFileReaderSchema, manifestFileReaderColumns,
      new ImmutableManifestScanFilters.Builder().build(), input.getEstimatedSize() + dataFileEstimatedCount, ManifestContent.DATA, true);
  }

  private Prel projectDataFileAndType(Prel manifestPrel) {
    // Project condition might not be correct
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_TYPE);
    Pair<Integer, RelDataTypeField> implicitFilePathCol = MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_PATH);
    Pair<Integer, RelDataTypeField> implicitFileTypeCol = MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_TYPE);
    Pair<Integer, RelDataTypeField> dataFilePathCol = MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> fileContentCol = MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_CONTENT);
    Preconditions.checkNotNull(implicitFilePathCol, "ManifestScan should always have implicitFilePath with rowType.");
    Preconditions.checkNotNull(implicitFileTypeCol, "ManifestScan should always have implicitFileType with rowType.");
    Preconditions.checkNotNull(dataFilePathCol, "ManifestScan should always have dataFileType with rowType.");
    Preconditions.checkNotNull(fileContentCol, "ManifestScan should always have fileContent with rowType.");

    RexBuilder rexBuilder = cluster.getRexBuilder();

    // if filePathCol is null, then use dataFilePathCol and fileContentCol values as file path and file type values.
    RexNode implicitFilePathNullCheck = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(implicitFilePathCol.right.getType(), implicitFilePathCol.left));

    RexNode filePathExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, implicitFilePathNullCheck,
      rexBuilder.makeInputRef(dataFilePathCol.right.getType(), dataFilePathCol.left),
      rexBuilder.makeInputRef(implicitFilePathCol.right.getType(), implicitFilePathCol.left));
    RexNode fileTypeExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, implicitFilePathNullCheck,
      rexBuilder.makeInputRef(fileContentCol.right.getType(), fileContentCol.left),
      rexBuilder.makeInputRef(implicitFileTypeCol.right.getType(), implicitFileTypeCol.left));

    final List<RexNode> projectExpressions = ImmutableList.of(filePathExpr, fileTypeExpr);
    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(manifestPrel.getCluster(), manifestPrel.getTraitSet(), manifestPrel, projectExpressions, newRowType);
  }

  private Prel projectFilePathAndType(Prel input) {
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_TYPE);
    Pair<Integer, RelDataTypeField> filePathCol = MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_PATH);
    Pair<Integer, RelDataTypeField> fileTypeCol = MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_TYPE);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode filePathExpr = rexBuilder.makeInputRef(filePathCol.right.getType(), filePathCol.left);
    RexNode fileContentExpr = rexBuilder.makeInputRef(fileTypeCol.right.getType(), fileTypeCol.left);

    final List<RexNode> projectExpressions = ImmutableList.of(filePathExpr, fileContentExpr);
    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(input.getCluster(), input.getTraitSet(), input, projectExpressions, newRowType);
  }

  private Prel reduceDuplicateFilePaths(Prel input) {
    AggregateCall aggOnFilePath = AggregateCall.create(
      SqlStdOperatorTable.COUNT,
      true,
      false,
      Collections.emptyList(),
      -1,
      RelCollations.EMPTY,
      1,
      input,
      input.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
      FILE_PATH
    );

    ImmutableBitSet groupSet = ImmutableBitSet.of(
      input.getRowType().getField(FILE_PATH, false, false).getIndex(),
      input.getRowType().getField(FILE_TYPE, false, false).getIndex());
    try {
      return HashAggPrel.create(
        input.getCluster(),
        input.getTraitSet(),
        input,
        groupSet,
        ImmutableList.of(groupSet),
        ImmutableList.of(aggOnFilePath),
        null
      );
    } catch (InvalidRelException e) {
      throw new RuntimeException("Failed to create HashAggPrel during delete file scan.", e);
    }
  }

  private Prel deleteOrphanFilesPlan(Prel input) {
    // We do overestimate instead of underestimate. 1) Use file counts from ALL snapshot; 2) consider every snapshot has partition stats files.
    long estimatedRows = dataFileEstimatedCount + manifestFileEstimatedCount + snapshotsCount /*Manifest list file*/ + snapshotsCount * 2 /*Partition stats files*/;
    return new IcebergOrphanFileDeletePrel(
      input.getCluster(), input.getTraitSet(), table, input, tableMetadata, estimatedRows);
  }

  private Prel outputSummaryPlan(Prel input) throws InvalidRelException {
    RelOptCluster cluster = input.getCluster();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    // Use single thread to collect deleted orphan files.
    input = new UnionExchangePrel(input.getCluster(), input.getTraitSet().plus(DistributionTrait.SINGLETON), input);

    // Projected conditions
    RexNode dataFileCondition = buildCaseCall(input, IcebergFileType.DATA);
    RexNode positionDeleteCondition = buildCaseCall(input, IcebergFileType.POSITION_DELETES);
    RexNode equalityDeleteCondition = buildCaseCall(input, IcebergFileType.EQUALITY_DELETES);
    RexNode manifestCondition = buildCaseCall(input, IcebergFileType.MANIFEST);
    RexNode manifestListCondition = buildCaseCall(input, IcebergFileType.MANIFEST_LIST);
    RexNode partitionStatsCondition = buildCaseCall(input, IcebergFileType.PARTITION_STATS);

    // Projected deleted data files
    RelDataType nullableBigInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
    List<RexNode> projectExpression = ImmutableList.of(dataFileCondition, positionDeleteCondition, equalityDeleteCondition,
      manifestCondition, manifestListCondition, partitionStatsCondition);

    List<String> summaryCols = VacuumOutputSchema.OUTPUT_SCHEMA.getFields().stream().map(Field::getName).collect(Collectors.toList());

    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = typeFactory.builder();
    summaryCols.forEach(c -> fieldInfoBuilder.add(c, nullableBigInt));
    RelDataType projectedRowType = fieldInfoBuilder.build();

    ProjectPrel project = ProjectPrel.create(cluster, traitSet, input, projectExpression, projectedRowType);

    // Aggregated summary
    List<AggregateCall> aggs = summaryCols.stream().map(c -> buildAggregateCall(project, projectedRowType, c)).collect(Collectors.toList());
    Prel agg = StreamAggPrel.create(cluster, project.getTraitSet(), project, ImmutableBitSet.of(), Collections.EMPTY_LIST, aggs, null);

    // Project: return 0 as row count in case there is no Agg record (i.e., no orphan files to delete)
    List<RexNode> projectExprs = summaryCols.stream().map(c -> notNullProjectExpr(agg, c)).collect(Collectors.toList());
    RelDataType projectRowType = RexUtil.createStructType(agg.getCluster().getTypeFactory(), projectExprs,
      summaryCols, null);
    return ProjectPrel.create(cluster, agg.getTraitSet(), agg, projectExprs, projectRowType);
  }

  private RexNode notNullProjectExpr(Prel input, String fieldName) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    final RexNode zeroLiteral = rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RelDataTypeField field = input.getRowType().getField(fieldName, false, false);
    RexInputRef inputRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());
    RexNode rowCountRecordExistsCheckCondition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);

    // case when the count of row count records is 0, return 0, else return aggregated row count
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, rowCountRecordExistsCheckCondition, zeroLiteral,
      rexBuilder.makeInputRef(field.getType(), field.getIndex()));
  }

  private AggregateCall buildAggregateCall(Prel relNode, RelDataType projectRowType, String fieldName) {
    RelDataTypeField aggField = projectRowType.getField(fieldName, false, false);
    return AggregateCall.create(
      SUM,
      false,
      false,
      ImmutableList.of(aggField.getIndex()),
      -1,
      RelCollations.EMPTY,
      1,
      relNode,
      cluster.getTypeFactory().createTypeWithNullability(cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true),
      fieldName);
  }

  private RexNode buildCaseCall(Prel orphanFileDeleteRel, IcebergFileType icebergFileType) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    Function<String, RexNode> makeLiteral = i -> rexBuilder.makeLiteral(i, typeFactory.createSqlType(VARCHAR), false);
    RelDataType nullableBigInt = typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);

    RelDataTypeField orphanFileTypeField = orphanFileDeleteRel.getRowType().getField(FILE_TYPE, false, false);
    RexInputRef orphanFileTypeIn = rexBuilder.makeInputRef(orphanFileTypeField.getType(), orphanFileTypeField.getIndex());
    RelDataTypeField recordsField = orphanFileDeleteRel.getRowType().getField(RECORDS, false, false);
    RexNode recordsIn = rexBuilder.makeCast(nullableBigInt, rexBuilder.makeInputRef(recordsField.getType(), recordsField.getIndex()));

    RexNode equalsCall = rexBuilder.makeCall(EQUALS, orphanFileTypeIn, makeLiteral.apply(icebergFileType.name()));
    return rexBuilder.makeCall(CASE, equalsCall, recordsIn, rexBuilder.makeZeroLiteral(nullableBigInt));
  }

  private DistributionTrait getHashDistributionTraitForFields(RelDataType rowType, List<String> columnNames) {
    ImmutableList<DistributionTrait.DistributionField> fields = columnNames.stream()
      .map(n -> new DistributionTrait.DistributionField(
        Preconditions.checkNotNull(rowType.getField(n, false, false)).getIndex()))
      .collect(ImmutableList.toImmutableList());
    return new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, fields);
  }

  /**
   * Utility function to apply IS_NULL(col) filter for the given input node
   */
  private Prel addColumnIsNullFilter(RelNode inputNode, RelDataType fieldType, int fieldIndex) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexNode filterCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.IS_NULL,
      rexBuilder.makeInputRef(fieldType, fieldIndex));

    return FilterPrel.create(
      inputNode.getCluster(),
      inputNode.getTraitSet(),
      inputNode,
      filterCondition);
  }

  /**
   * Here is a suboptimal plan to estimate the row accounts for Prels used in ExpireSnapshots plan. The 'suboptimal' mean
   * to directly load the Iceberg table and read back its all snapshots and stats of each snapshot for row estimates.
   * Another approach is tracked in DX-63280.
   */
  private void loadIcebergTable() {
    if (icebergTable == null) {
      IcebergTableProps icebergTableProps = createTableEntry.getIcebergTableProps();
      Preconditions.checkState(createTableEntry.getPlugin() instanceof SupportsIcebergMutablePlugin, "Plugin not instance of SupportsIcebergMutablePlugin");
      SupportsIcebergMutablePlugin plugin = (SupportsIcebergMutablePlugin) createTableEntry.getPlugin();
      try (FileSystem fs = plugin.createFS(icebergTableProps.getTableLocation(), createTableEntry.getUserName(), null)) {
        IcebergModel icebergModel = plugin.getIcebergModel(icebergTableProps, createTableEntry.getUserName(), null, fs);
        icebergTable = icebergModel.getIcebergTable(icebergModel.getTableIdentifier(icebergTableProps.getTableLocation()));
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    Iterator<Snapshot> iterator = icebergTable.snapshots().iterator();
    while (iterator.hasNext()) {
      Snapshot snapshot = iterator.next();
      snapshotsCount++;
      estimateFilesFromSnapshot(snapshot, snapshotsCount);
    }

    if (snapshotsCount == 1 || vacuumOptions.getRetainLast() >= snapshotsCount) {
      throw UserException.unsupportedError()
        .message("Vacuum table succeeded, and the operation did not change the number of snapshots.")
        .buildSilently();
    }
  }

  private void estimateFilesFromSnapshot(Snapshot snapshot, long snapshotsCount) {
    // First snapshot
    if (1 == snapshotsCount) {
      long numDataFiles = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-data-files", "0")) : 0L;
      dataFileEstimatedCount += numDataFiles;
      long numPositionDeletes = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-position-deletes", "0")) : 0L;
      dataFileEstimatedCount += numPositionDeletes;
      long numEqualityDeletes = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault("total-equality-deletes", "0")) : 0L;
      dataFileEstimatedCount += numEqualityDeletes;

      manifestFileEstimatedCount += Math.max(dataFileEstimatedCount / ESTIMATED_RECORDS_PER_MANIFEST, 1);
    } else {
      long numAddedDataFiles = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault(ADDED_DATA_FILES, "0")) : 0L;
      dataFileEstimatedCount += numAddedDataFiles;
      long numAddedDeleteFiles = snapshot != null ?
        Long.parseLong(snapshot.summary().getOrDefault(DELETED_DATA_FILES, "0")) : 0L;
      dataFileEstimatedCount += numAddedDeleteFiles;

      manifestFileEstimatedCount += Math.max((numAddedDataFiles + numAddedDeleteFiles) / ESTIMATED_RECORDS_PER_MANIFEST, 1);
    }
  }
}
