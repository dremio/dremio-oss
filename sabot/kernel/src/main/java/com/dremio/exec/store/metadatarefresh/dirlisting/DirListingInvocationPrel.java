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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergManifestListPrel;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.options.Options;
import com.dremio.sabot.op.join.JoinUtils;
import com.google.common.collect.ImmutableList;

@Options
public class DirListingInvocationPrel extends ScanPrelBase implements Prel, PrelFinalizable, Rel {
  private static final List<SchemaPath> PROJECTED_COLS = MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields()
    .stream()
    .map(Field::getName)
    .map(SchemaPath::getSimplePath)
    .collect(Collectors.toList());

  private FileSystemPlugin<?> metaStoragePlugin;
  private String tableUUID;

  public DirListingInvocationPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                                  TableMetadata tableMetadata, double observedRowcountAdjustment, FileSystemPlugin<?> plugin, String uuid) {
    super(cluster, traitSet, table, pluginId, tableMetadata, PROJECTED_COLS, observedRowcountAdjustment);
    this.metaStoragePlugin = plugin;
    this.tableUUID = uuid;
  }

  @Override
  public DirListingInvocationPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DirListingInvocationPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment, metaStoragePlugin, tableUUID);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new DirListingInvocationPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment, metaStoragePlugin, tableUUID);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return null;
  }

  /*                     +------------------------------------------------------------+
   *                     |            Project                                         |
   *                     |   coalesce(filePath != null then filePath else dataFilePath|
   *                     +------------------------------------------------------------+
   *                                       |
   *                     +------------------------------------------+
   *                     |           Filter                         |
   *                     | filePath is null or dataFilePath is null |
   *                     +------------------------------------------+
   *                                      |
   *                     +--------------------------------------+
   *                     |            HashJoin                  |
   *                     | full join on filePath = datafilePath |
   *                     +--------------------------------------+
   *                                      |
   *               |------------------------------------------------------------------------|
   *               |                                                                        |
   * +----------------------------+                                             +----------------------------+
   * |  HashToRandomExchangePrel  |                                             |  HashToRandomExchangePrel  |
   * +----------------------------+                                             +----------------------------+
   *               |                                                                        |
   *               |                                                                        |
   *               |                                                                        |
   *               |                                                                        |
   *               |                                                 +--------------------------------------------------------------------+
   *               |                                                 |               ManifestScanTableFunction                            |
   *               |                                                 |        (generate dataFilePath by reading manifestFiles)            |
   *               |                                                 +--------------------------------------------------------------------+
   *               |                                                                        |
   *               |                                                                        |
   * +------------------------------------+                         +--------------------------------------------------------------------+
   * |         DirListScan                |                         |         IcebergManifestListScan                                    |
   * |  (list files as filePath)          |                         |  (Scan manifest list file to generate manifest files)              |
   * +------------------------------------+                         +--------------------------------------------------------------------+
   */
  @Override
  public Prel finalizeRel() {
    Prel manifestListPrel = generateManifestListScanPrel();
    Prel manifestReadPrel = generateManifestFileReadScanPrel(manifestListPrel);

    Prel dirListingScanPrel = new DirListingScanPrel(getCluster(), traitSet, table, pluginId, tableMetadata, getObservedRowcountAdjustment());

    Prel hashJoinPrel = addHashJoin(dirListingScanPrel, manifestReadPrel);
    Prel filterAlreadyPresetFilesPrel = addFilterForAlreadyPresetFiles(hashJoinPrel);

    Prel finalPrel = coaleseToOnePathProject(filterAlreadyPresetFilesPrel);
    return finalPrel;
  }

  private Prel generateManifestListScanPrel() {
    BatchSchema manifestListReaderSchema = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

    List<SchemaPath> manifestListReaderColumns = TableFunctionUtil.getSplitGenSchemaColumns();
    final RelDataType manifestListRowType = getRowTypeFromProjectedColumns(manifestListReaderColumns, manifestListReaderSchema, getCluster());

    InternalIcebergScanTableMetadata icebergScanTableMetadata = new InternalIcebergScanTableMetadata(tableMetadata, metaStoragePlugin, tableUUID);

    IcebergManifestListPrel manifestListPrel = new IcebergManifestListPrel(getCluster(), traitSet, icebergScanTableMetadata, manifestListReaderSchema, manifestListReaderColumns,
      manifestListRowType);
    return manifestListPrel;
  }

  private Prel generateManifestFileReadScanPrel(Prel manifestListPrel) {
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    // exchange above manifest list scan, which is a leaf level easy scan
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
      manifestListPrel, distributionTrait.getFields(), true);

    List<SchemaPath> manifestFileReaderColumns = new ArrayList<>(Arrays.asList(SchemaPath.getSimplePath(RecordReader.DATAFILE_PATH)));
    BatchSchema manifestFileReaderSchema = RecordReader.MANIFEST_SCAN_TABLE_FUNCTION_SCHEMA;

    // Manifest scan phase
    TableFunctionConfig manifestScanTableFunctionConfig = TableFunctionUtil.getInternalMetadataManifestScanTableFunctionConfig(tableMetadata, manifestFileReaderColumns,
      manifestFileReaderSchema, null);

    final RelDataType outputRowType = getRowTypeFromProjectedColumns(manifestFileReaderColumns, manifestFileReaderSchema, getCluster());

    TableFunctionPrel manifestScanTF = new TableFunctionPrel(getCluster(), traitSet, table, manifestSplitsExchange,
      tableMetadata, ImmutableList.copyOf(manifestFileReaderColumns), manifestScanTableFunctionConfig, outputRowType, TableFunctionPrel.TableFunctionPOPCreator.DEFAULT);

    return manifestScanTF;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  private Pair<Integer, RelDataTypeField> findFieldWithIndex(RelNode relNode, String fieldName) {
    Pair<Integer, RelDataTypeField> fieldPair = MoreRelOptUtil.findFieldWithIndex(relNode.getRowType().getFieldList(), fieldName);

    if (fieldPair == null) {
      throw new RuntimeException(String.format("Unable to find field '%s' in the schema", fieldName));
    }
    return fieldPair;
  }

  public double getObservedRowcountAdjustment() {
    return 1.0;
  }

  private Prel addHashJoin(Prel dirListingScanPrel, Prel icebergScanPrel) {
    RexBuilder rexBuilder = getCluster().getRexBuilder();
    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    dirListingScanPrel = new HashToRandomExchangePrel(dirListingScanPrel.getCluster(), relTraitSet,
      dirListingScanPrel, distributionTrait.getFields());

    icebergScanPrel = new HashToRandomExchangePrel(icebergScanPrel.getCluster(), relTraitSet,
      icebergScanPrel, distributionTrait.getFields());

    Pair<Integer, RelDataTypeField> leftSidePath = findFieldWithIndex(dirListingScanPrel, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    Pair<Integer, RelDataTypeField> rightSidePath = findFieldWithIndex(icebergScanPrel, RecordReader.DATAFILE_PATH);

    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(leftSidePath.right.getType(), leftSidePath.left),
      rexBuilder.makeInputRef(rightSidePath.right.getType(), dirListingScanPrel.getRowType().getFieldCount() + rightSidePath.left));

    HashJoinPrel hashJoinPrel = HashJoinPrel.create(getCluster(), traitSet, dirListingScanPrel, icebergScanPrel,
      joinCondition, JoinRelType.FULL, JoinUtils.projectAll(dirListingScanPrel.getRowType().getFieldCount() + icebergScanPrel.getRowType().getFieldCount()));

    return hashJoinPrel;
  }

  private Prel addFilterForAlreadyPresetFiles(Prel hashJoinPrel) {
    RexBuilder rexBuilder = getCluster().getRexBuilder();

    Pair<Integer, RelDataTypeField> dirListPath = findFieldWithIndex(hashJoinPrel, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    Pair<Integer, RelDataTypeField> icebergScanPath = findFieldWithIndex(hashJoinPrel, RecordReader.DATAFILE_PATH);

    RexNode dirListPathRef = rexBuilder.makeInputRef(dirListPath.right.getType(), dirListPath.left);
    RexNode icebergScanPathRef = rexBuilder.makeInputRef(icebergScanPath.right.getType(), icebergScanPath.left);

    RexNode isNullDirListPath = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, dirListPathRef);
    RexNode isNullIcebergPath = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, icebergScanPathRef);

    RexNode isNotEqualDirListAndIcebergPath = rexBuilder.makeCall(SqlStdOperatorTable.OR, isNullDirListPath, isNullIcebergPath);
    FilterPrel filterAlreadyExisting = FilterPrel.create(getCluster(), traitSet, hashJoinPrel, isNotEqualDirListAndIcebergPath);

    return filterAlreadyExisting;
  }

  private Prel coaleseToOnePathProject(Prel filterAlreadyPresetFilesPrel) {
    RexBuilder rexBuilder = getCluster().getRexBuilder();

    Pair<Integer, RelDataTypeField> dirListPath = findFieldWithIndex(filterAlreadyPresetFilesPrel, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    Pair<Integer, RelDataTypeField> icebergScanPath = findFieldWithIndex(filterAlreadyPresetFilesPrel, RecordReader.DATAFILE_PATH);

    RexInputRef dirListPathRef = rexBuilder.makeInputRef(dirListPath.right.getType(), dirListPath.left);
    RexInputRef icebergScanPathRef = rexBuilder.makeInputRef(icebergScanPath.right.getType(), icebergScanPath.left);

    //dirListPath != null
    RexNode isNotNullDirListPath = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, dirListPathRef);

//  case(dataFilePath != null) then dataFilePath else dirListPath
    RexNode coalesce = rexBuilder.makeCall(SqlStdOperatorTable.CASE, isNotNullDirListPath,
      dirListPathRef, icebergScanPathRef);

    RexNode isDeleted = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, dirListPathRef);

    List<RexNode> projectExpressions = new ArrayList<>();
    List<String> projectFields = new ArrayList<>();

    projectExpressions.add(coalesce);
    projectExpressions.add(isDeleted);
    projectFields.add(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    projectFields.add(MetadataRefreshExecConstants.isDeletedFile);

    List<Field> dirListInputFields = MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields();

    dirListInputFields.forEach(x -> {
      if (x.getName() != MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH) {
        projectFields.add(x.getName());
        Pair<Integer, RelDataTypeField> tempRef = findFieldWithIndex(filterAlreadyPresetFilesPrel, x.getName());
        projectExpressions.add(rexBuilder.makeInputRef(tempRef.right.getType(), tempRef.left));
      }
    });

    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);
    ProjectPrel projectPrel = ProjectPrel.create(getCluster(), filterAlreadyPresetFilesPrel.getTraitSet(), filterAlreadyPresetFilesPrel, projectExpressions, newRowType);

    return projectPrel;
  }
}
