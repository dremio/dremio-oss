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

import static com.dremio.exec.planner.physical.visitor.WriterUpdater.getCollation;
import static com.dremio.exec.store.RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_CONTENT;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

import com.dremio.common.JSONOptions;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.ValuesPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergManifestListScanPrel;
import com.dremio.exec.store.iceberg.IcebergManifestScanPrel;
import com.dremio.exec.store.iceberg.IcebergSnapshotsPrel;
import com.dremio.exec.store.iceberg.ManifestFileDuplicateRemovePrel;
import com.dremio.exec.store.iceberg.PartitionStatsScanPrel;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
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

/** Expand plans for VACUUM TABLE. */
public abstract class VacuumPlanGenerator {
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected final RelOptCluster cluster;
  protected final RelTraitSet traitSet;
  protected final VacuumOptions vacuumOptions;
  protected final String user;
  protected final StoragePluginId internalStoragePlugin;
  protected final StoragePluginId storagePluginId;
  protected final List<PartitionChunkMetadata> splits;
  protected final IcebergCostEstimates icebergCostEstimates;

  protected final BatchSchema MANIFEST_SCAN_SCHEMA =
      BatchSchema.newBuilder() // Sub-schema with only applicable fields
          .addField(Field.nullable(DATAFILE_PATH, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(FILE_CONTENT, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
          .addField(Field.nullable(FILE_TYPE, Types.MinorType.VARCHAR.getType()))
          .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
          .build();

  public VacuumPlanGenerator(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<PartitionChunkMetadata> splits,
      IcebergCostEstimates icebergCostEstimates,
      VacuumOptions vacuumOptions,
      StoragePluginId internalStoragePlugin,
      StoragePluginId storagePluginId,
      String user) {
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.icebergCostEstimates = icebergCostEstimates;
    this.vacuumOptions = Preconditions.checkNotNull(vacuumOptions, "VacuumOption cannot be null.");
    this.user = user;
    this.internalStoragePlugin = internalStoragePlugin;
    this.storagePluginId = storagePluginId;
    this.splits = splits;
  }

  public abstract Prel buildPlan();

  protected Prel orphanFilesPlan(Prel expired, Prel live) {
    Prel joinPlan = joinLiveAndExpiredPlan(expired, live);
    // Need to count left side fields.
    final int leftFieldCount = joinPlan.getInput(0).getRowType().getFieldCount();

    // Orphan file paths: right.FILE_PATH IS NULL
    RelDataTypeField rightRowIndexField =
        joinPlan.getInput(1).getRowType().getField(FILE_PATH, false, false);

    return addColumnIsNullFilter(
        joinPlan, rightRowIndexField.getType(), leftFieldCount + rightRowIndexField.getIndex());
  }

  private Prel joinLiveAndExpiredPlan(Prel expired, Prel live) {
    RexBuilder rexBuilder = expired.getCluster().getRexBuilder();

    // Left side: Source files from expired snapshots
    DistributionTrait leftDistributionTrait =
        getHashDistributionTraitForFields(expired.getRowType(), ImmutableList.of(FILE_PATH));
    RelTraitSet leftTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(leftDistributionTrait);
    HashToRandomExchangePrel leftSourceFilePathHashExchange =
        new HashToRandomExchangePrel(
            cluster, leftTraitSet, expired, leftDistributionTrait.getFields());

    // Right side: Source files from live snapshots
    DistributionTrait rightDistributionTrait =
        getHashDistributionTraitForFields(live.getRowType(), ImmutableList.of(FILE_PATH));
    RelTraitSet rightTraitSet =
        cluster.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(rightDistributionTrait);
    HashToRandomExchangePrel rightSourceFilePathHashExchange =
        new HashToRandomExchangePrel(
            cluster, rightTraitSet, live, rightDistributionTrait.getFields());

    // hash join on FILE_PATH == FILE_PATH

    RelDataTypeField leftSourceFilePathField =
        leftSourceFilePathHashExchange.getRowType().getField(FILE_PATH, false, false);
    RelDataTypeField rightSourceFilePathField =
        rightSourceFilePathHashExchange.getRowType().getField(FILE_PATH, false, false);

    int leftFieldCount = leftSourceFilePathHashExchange.getRowType().getFieldCount();
    RexNode joinCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(
                leftSourceFilePathField.getType(), leftSourceFilePathField.getIndex()),
            rexBuilder.makeInputRef(
                rightSourceFilePathField.getType(),
                leftFieldCount + rightSourceFilePathField.getIndex()));

    return HashJoinPrel.create(
        leftSourceFilePathHashExchange.getCluster(),
        leftSourceFilePathHashExchange.getTraitSet(),
        leftSourceFilePathHashExchange,
        rightSourceFilePathHashExchange,
        joinCondition,
        null,
        JoinRelType.LEFT,
        true);
  }

  protected Prel deDupFilePathAndTypeScanPlan(Prel snapshotsScan)
      throws InvalidRelException, IOException {
    Prel manifestPlan = filePathAndTypePlanFromManifest(snapshotsScan);
    Prel filePathAndTypeProject = projectDataFileAndType(manifestPlan);
    Prel deDupFilePathPlan = reduceDuplicateFilePaths(filePathAndTypeProject);
    return projectFilePathAndType(deDupFilePathPlan);
  }

  protected Prel filePathAndTypeScanPlan(Prel snapshotsScan) throws InvalidRelException {
    Prel manifestPlan = filePathAndTypePlanFromManifest(snapshotsScan);
    Prel filePathAndTypeProject = projectDataFileAndType(manifestPlan);
    return projectFilePathAndType(filePathAndTypeProject);
  }

  protected Prel filePathAndTypePlanFromManifest(Prel snapshotsScanPlan) {
    Prel partitionStatsScan = getPartitionStatsScanPrel(snapshotsScanPlan);
    Prel manifestListScan = getManifestListScanPrel(partitionStatsScan);
    // Sort the manifest file paths and remove the duplicate manifest file paths
    Prel manifestFilePathsSortedPlan = sortManifestFilePathsPlan(manifestListScan);
    Prel manifestFileDuplicateRemovePlan =
        manifestFileDuplicateRemovePlan(manifestFilePathsSortedPlan);
    return getManifestScanPrel(manifestFileDuplicateRemovePlan);
  }

  protected Prel snapshotsScanPlan(SnapshotsScanOptions.Mode scanMode) {
    SnapshotsScanOptions snapshotsOption =
        new SnapshotsScanOptions(
            scanMode, vacuumOptions.getOlderThanInMillis(), vacuumOptions.getRetainLast());
    return new IcebergSnapshotsPrel(
        cluster,
        traitSet,
        snapshotsOption,
        user,
        storagePluginId,
        splits.iterator(),
        icebergCostEstimates.getSnapshotsCount(),
        1,
        getSchemeVariate());
  }

  protected String getSchemeVariate() {
    return null;
  }

  protected Prel getManifestListScanPrel(Prel input) {
    BatchSchema manifestListsReaderSchema =
        SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    List<SchemaPath> manifestListsReaderColumns =
        manifestListsReaderSchema.getFields().stream()
            .map(f -> SchemaPath.getSimplePath(f.getName()))
            .collect(Collectors.toList());

    // Estimate rows = number of snapshots * number of files per snapshots
    long estimatedRows =
        icebergCostEstimates.getEstimatedRows() * icebergCostEstimates.getSnapshotsCount();
    return new IcebergManifestListScanPrel(
        storagePluginId,
        input.getCluster(),
        input.getTraitSet(),
        input,
        manifestListsReaderSchema,
        manifestListsReaderColumns,
        estimatedRows,
        user,
        getSchemeVariate());
  }

  protected Prel getPartitionStatsScanPrel(Prel input) {
    BatchSchema partitionStatsScanSchema =
        ICEBERG_SNAPSHOTS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
    // TODO: it could be further improved whether it needs to apply PartitionStatsScan, if table is
    // written by other engines, or the partition stats metadata entry is not present.

    long estimatedRows = 2L * input.getEstimatedSize();
    return new PartitionStatsScanPrel(
        storagePluginId,
        input.getCluster(),
        input.getTraitSet(),
        input,
        partitionStatsScanSchema,
        estimatedRows,
        user,
        enableCarryForwardOnPartitionStats(),
        getSchemeVariate());
  }

  protected abstract boolean enableCarryForwardOnPartitionStats();

  protected Prel getManifestScanPrel(Prel input) {
    DistributionTrait.DistributionField distributionField =
        new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait =
        new DistributionTrait(
            DistributionTrait.DistributionType.HASH_DISTRIBUTED,
            ImmutableList.of(distributionField));
    Prel manifestSplitsExchange =
        new HashToRandomExchangePrel(
            input.getCluster(),
            input.getTraitSet(),
            input,
            distributionTrait.getFields(),
            TableFunctionUtil.getTableAgnosticHashExchangeTableFunctionCreator(
                storagePluginId, user));

    BatchSchema manifestFileReaderSchema = MANIFEST_SCAN_SCHEMA;
    List<SchemaPath> manifestFileReaderColumns =
        manifestFileReaderSchema.getFields().stream()
            .map(f -> SchemaPath.getSimplePath(f.getName()))
            .collect(Collectors.toList());

    RelDataType rowType =
        ScanRelBase.getRowTypeFromProjectedColumns(
            manifestFileReaderColumns, manifestFileReaderSchema, cluster);

    return new IcebergManifestScanPrel(
        manifestSplitsExchange.getCluster(),
        manifestSplitsExchange.getTraitSet().plus(DistributionTrait.ANY),
        manifestSplitsExchange,
        storagePluginId,
        internalStoragePlugin,
        manifestFileReaderColumns,
        manifestFileReaderSchema,
        rowType,
        input.getEstimatedSize() + icebergCostEstimates.getDataFileEstimatedCount(),
        user,
        false,
        getSchemeVariate());
  }

  private Prel projectDataFileAndType(Prel manifestPrel) {
    // Project condition might not be correct
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_TYPE);
    Pair<Integer, RelDataTypeField> implicitFilePathCol =
        MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_PATH);
    Pair<Integer, RelDataTypeField> implicitFileTypeCol =
        MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_TYPE);
    Pair<Integer, RelDataTypeField> dataFilePathCol =
        MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> fileContentCol =
        MoreRelOptUtil.findFieldWithIndex(manifestPrel.getRowType().getFieldList(), FILE_CONTENT);
    Preconditions.checkNotNull(
        implicitFilePathCol, "ManifestScan should always have implicitFilePath with rowType.");
    Preconditions.checkNotNull(
        dataFilePathCol, "ManifestScan should always have dataFileType with rowType.");
    Preconditions.checkNotNull(
        fileContentCol, "ManifestScan should always have fileContent with rowType.");

    RexBuilder rexBuilder = cluster.getRexBuilder();

    // if filePathCol is null, then use dataFilePathCol and fileContentCol values as file path and
    // file type values.
    RexNode implicitFilePathNullCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            rexBuilder.makeInputRef(implicitFilePathCol.right.getType(), implicitFilePathCol.left));

    RexNode filePathExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            implicitFilePathNullCheck,
            rexBuilder.makeInputRef(dataFilePathCol.right.getType(), dataFilePathCol.left),
            rexBuilder.makeInputRef(implicitFilePathCol.right.getType(), implicitFilePathCol.left));
    RexNode fileTypeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            implicitFilePathNullCheck,
            rexBuilder.makeInputRef(fileContentCol.right.getType(), fileContentCol.left),
            rexBuilder.makeInputRef(implicitFileTypeCol.right.getType(), implicitFileTypeCol.left));

    final List<RexNode> projectExpressions = ImmutableList.of(filePathExpr, fileTypeExpr);
    RelDataType newRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            projectFields,
            SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(
        manifestPrel.getCluster(),
        manifestPrel.getTraitSet(),
        manifestPrel,
        projectExpressions,
        newRowType);
  }

  protected Prel projectFilePathAndType(Prel input) {
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_TYPE);
    Pair<Integer, RelDataTypeField> filePathCol =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_PATH);
    Pair<Integer, RelDataTypeField> fileTypeCol =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_TYPE);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode filePathExpr = rexBuilder.makeInputRef(filePathCol.right.getType(), filePathCol.left);
    RexNode fileContentExpr =
        rexBuilder.makeInputRef(fileTypeCol.right.getType(), fileTypeCol.left);

    final List<RexNode> projectExpressions = ImmutableList.of(filePathExpr, fileContentExpr);
    RelDataType newRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            projectFields,
            SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(
        input.getCluster(), input.getTraitSet(), input, projectExpressions, newRowType);
  }

  private Prel reduceDuplicateFilePaths(Prel input) throws InvalidRelException {
    AggregateCall aggOnFilePath =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            Collections.emptyList(),
            -1,
            RelCollations.EMPTY,
            1,
            input,
            input.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT),
            FILE_PATH);

    ImmutableBitSet groupSet =
        ImmutableBitSet.of(
            input.getRowType().getField(FILE_PATH, false, false).getIndex(),
            input.getRowType().getField(FILE_TYPE, false, false).getIndex());
    return HashAggPrel.create(
        input.getCluster(),
        input.getTraitSet(),
        input,
        groupSet,
        ImmutableList.of(groupSet),
        ImmutableList.of(aggOnFilePath),
        null);
  }

  protected abstract Prel deleteOrphanFilesPlan(Prel input);

  protected abstract Prel outputSummaryPlan(Prel input) throws InvalidRelException;

  protected Prel outputZerosPlan() throws InvalidRelException {
    List<String> summaryCols =
        VacuumOutputSchema.EXPIRE_SNAPSHOTS_OUTPUT_SCHEMA.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    RelDataTypeFactory.FieldInfoBuilder fieldInfoBuilder = typeFactory.builder();
    RelDataType nullableBigInt =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);
    summaryCols.forEach(c -> fieldInfoBuilder.add(c, nullableBigInt));
    RelDataType rowType = fieldInfoBuilder.build();

    ObjectNode successMessage = OBJECT_MAPPER.createObjectNode();
    summaryCols.forEach(c -> successMessage.set(c, new IntNode(0)));

    return new ValuesPrel(cluster, traitSet, rowType, new JSONOptions(successMessage), 1d);
  }

  protected RexNode notNullProjectExpr(Prel input, String fieldName) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    final RexNode zeroLiteral =
        rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RelDataTypeField field = input.getRowType().getField(fieldName, false, false);
    RexInputRef inputRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());
    RexNode rowCountRecordExistsCheckCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);

    // case when the count of row count records is 0, return 0, else return aggregated row count
    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        rowCountRecordExistsCheckCondition,
        zeroLiteral,
        rexBuilder.makeInputRef(field.getType(), field.getIndex()));
  }

  protected AggregateCall buildAggregateCall(
      Prel relNode, RelDataType projectRowType, String fieldName) {
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
        cluster
            .getTypeFactory()
            .createTypeWithNullability(
                cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT), true),
        fieldName);
  }

  protected DistributionTrait getHashDistributionTraitForFields(
      RelDataType rowType, List<String> columnNames) {
    ImmutableList<DistributionTrait.DistributionField> fields =
        columnNames.stream()
            .map(
                n ->
                    new DistributionTrait.DistributionField(
                        Preconditions.checkNotNull(rowType.getField(n, false, false)).getIndex()))
            .collect(ImmutableList.toImmutableList());
    return new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, fields);
  }

  /** Utility function to apply IS_NULL(col) filter for the given input node */
  protected Prel addColumnIsNullFilter(RelNode inputNode, RelDataType fieldType, int fieldIndex) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexNode filterCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef(fieldType, fieldIndex));

    return FilterPrel.create(
        inputNode.getCluster(), inputNode.getTraitSet(), inputNode, filterCondition);
  }

  /***
   * Sort the plan by manifest file path and file type so that it can remove the duplicate manifest file paths
   */
  private Prel sortManifestFilePathsPlan(Prel input) {
    RelDataTypeField filePathField = input.getRowType().getField(FILE_PATH, false, false);
    RelDataTypeField fileTypeField = input.getRowType().getField(FILE_TYPE, false, false);

    final RelCollation collation =
        getCollation(
            input.getTraitSet(),
            ImmutableList.of(filePathField.getIndex(), fileTypeField.getIndex()));
    Prel sortPrel =
        SortPrel.create(input.getCluster(), input.getTraitSet().plus(collation), input, collation);
    return sortPrel;
  }

  private Prel manifestFileDuplicateRemovePlan(Prel input) {
    // Estimate rows = number of snapshots * number of files per snapshots
    long estimatedRows =
        icebergCostEstimates.getEstimatedRows() * icebergCostEstimates.getSnapshotsCount();
    return new ManifestFileDuplicateRemovePrel(
        input.getCluster(), input.getTraitSet(), input, estimatedRows);
  }
}
