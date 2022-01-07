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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import com.dremio.common.utils.PathUtils;
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
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.IcebergManifestListPrel;
import com.dremio.exec.store.iceberg.InternalIcebergScanTableMetadata;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.options.Options;
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
  private boolean isPartialRefresh;
  private UnlimitedSplitsMetadataProvider metadataProvider;
  private List<String> partialRefreshPaths;
  private Function<RelMetadataQuery, Double> estimateRowCountFn;

  public DirListingInvocationPrel(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                                  TableMetadata tableMetadata, double observedRowcountAdjustment, FileSystemPlugin<?> plugin, String uuid, boolean isPartialRefresh, UnlimitedSplitsMetadataProvider metadataProvider, List<String> partialRefreshPaths,
                                  final Function<RelMetadataQuery, Double> estimateRowCountFn) {
    super(cluster, traitSet, table, pluginId, tableMetadata, PROJECTED_COLS, observedRowcountAdjustment);
    this.metaStoragePlugin = plugin;
    this.tableUUID = uuid;
    this.isPartialRefresh = isPartialRefresh;
    this.metadataProvider = metadataProvider;
    this.partialRefreshPaths = partialRefreshPaths;
    this.estimateRowCountFn = estimateRowCountFn;
  }

  @Override
  public DirListingInvocationPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DirListingInvocationPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment, metaStoragePlugin, tableUUID, isPartialRefresh, metadataProvider, partialRefreshPaths, estimateRowCountFn);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
    return new DirListingInvocationPrel(getCluster(), traitSet, table, pluginId, tableMetadata, observedRowcountAdjustment, metaStoragePlugin, tableUUID, isPartialRefresh, metadataProvider, partialRefreshPaths, estimateRowCountFn);
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
    InternalIcebergScanTableMetadata icebergScanTableMetadata = new InternalIcebergScanTableMetadata(tableMetadata, metaStoragePlugin, tableUUID);
    Prel manifestListPrel = generateManifestListScanPrel(icebergScanTableMetadata);
    Prel manifestReadPrel = generateManifestFileReadScanPrel(manifestListPrel, icebergScanTableMetadata);

    Prel dirListingScanPrel = new DirListingScanPrel(getCluster(), traitSet, table, pluginId, tableMetadata, getObservedRowcountAdjustment(), !isPartialRefresh, estimateRowCountFn);

    Prel hashJoinPrel = addHashJoin(dirListingScanPrel, manifestReadPrel);
    Prel filterAlreadyPresetFilesPrel = addFilterForAlreadyPresetFiles(hashJoinPrel);

    Prel finalPrel = coaleseToOnePathProject(filterAlreadyPresetFilesPrel);
    return finalPrel;
  }

  private Prel generateManifestListScanPrel(TableMetadata icebergScanTableMetadata) {
    BatchSchema manifestListReaderSchema = RecordReader.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA;

    List<SchemaPath> manifestListReaderColumns = TableFunctionUtil.getSplitGenSchemaColumns();
    final RelDataType manifestListRowType = getRowTypeFromProjectedColumns(manifestListReaderColumns, manifestListReaderSchema, getCluster());

    IcebergManifestListPrel manifestListPrel = new IcebergManifestListPrel(getCluster(), traitSet, icebergScanTableMetadata, manifestListReaderSchema, manifestListReaderColumns,
      manifestListRowType, null); //TODO: check icebergExpression can be null or not
    return manifestListPrel;
  }

  private Prel generateManifestFileReadScanPrel(Prel manifestListPrel, TableMetadata icebergScanTableMetadata) {
    RexBuilder rexBuilder = getCluster().getRexBuilder();

    DistributionTrait.DistributionField distributionField = new DistributionTrait.DistributionField(0);
    DistributionTrait distributionTrait = new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.of(distributionField));
    RelTraitSet relTraitSet = getCluster().getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distributionTrait);

    // exchange above manifest list scan, which is a leaf level easy scan
    HashToRandomExchangePrel manifestSplitsExchange = new HashToRandomExchangePrel(getCluster(), relTraitSet,
      manifestListPrel, distributionTrait.getFields(), TableFunctionUtil.getHashExchangeTableFunctionCreator(icebergScanTableMetadata, true));


    BatchSchema manifestFileReaderSchema = MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.BATCH_SCHEMA;

    List<SchemaPath> manifestFileReaderColumns = manifestFileReaderSchema.getFields().stream().map(x ->
      SchemaPath.getSimplePath(x.getName())).collect(Collectors.toList());

    // Manifest scan phase
    TableFunctionConfig manifestScanTableFunctionConfig = TableFunctionUtil.getInternalMetadataManifestScanTableFunctionConfig(icebergScanTableMetadata, manifestFileReaderColumns,
      manifestFileReaderSchema, null);

    final RelDataType outputRowType = getRowTypeFromProjectedColumns(manifestFileReaderColumns, manifestFileReaderSchema, getCluster());

    TableFunctionPrel manifestScanTF = new TableFunctionPrel(getCluster(), traitSet, table, manifestSplitsExchange,
      tableMetadata, ImmutableList.copyOf(manifestFileReaderColumns), manifestScanTableFunctionConfig, outputRowType);

    if (isPartialRefresh) {
      return addFilterForFilteringPerPartition(manifestScanTF);
    }
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
    Pair<Integer, RelDataTypeField> rightSidePath = findFieldWithIndex(icebergScanPrel, MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.DATAFILE_PATH);

    RexNode joinCondition = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeInputRef(leftSidePath.right.getType(), leftSidePath.left),
      rexBuilder.makeInputRef(rightSidePath.right.getType(), dirListingScanPrel.getRowType().getFieldCount() + rightSidePath.left));

    HashJoinPrel hashJoinPrel = HashJoinPrel.create(getCluster(), traitSet, dirListingScanPrel, icebergScanPrel,
      joinCondition, null, JoinRelType.FULL);

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

    Pair<Integer, RelDataTypeField> dirListPartition = findFieldWithIndex(filterAlreadyPresetFilesPrel, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO);
    Pair<Integer, RelDataTypeField> icebergScanPartition = findFieldWithIndex(filterAlreadyPresetFilesPrel, MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.PARTITION_DATA_PATH);

    RexInputRef dirListPartitionRef = rexBuilder.makeInputRef(dirListPartition.right.getType(), dirListPartition.left);
    RexInputRef icebergScanPartitionRef = rexBuilder.makeInputRef(icebergScanPartition.right.getType(), icebergScanPartition.left);

    RexNode isNotNullDirListPart = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, dirListPartitionRef);

    RexNode combinePartitionInfo = rexBuilder.makeCall(SqlStdOperatorTable.CASE, isNotNullDirListPart,
      dirListPartitionRef, icebergScanPartitionRef);

    List<RexNode> projectExpressions = new ArrayList<>();
    List<String> projectFields = new ArrayList<>();

    projectExpressions.add(coalesce);
    projectExpressions.add(combinePartitionInfo);
    projectExpressions.add(isDeleted);

    projectFields.add(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    projectFields.add(MetadataRefreshExecConstants.PARTITION_INFO);
    projectFields.add(MetadataRefreshExecConstants.IS_DELETED_FILE);

    List<Field> dirListInputFields = MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.BATCH_SCHEMA.getFields();

    List<String> ignoreRefs = ImmutableList.of(MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH,
      MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.PARTITION_INFO,
      MetadataRefreshExecConstants.PathGeneratingDataFileProcessor.OUTPUT_SCHEMA.PARTITION_DATA_PATH);

    dirListInputFields.forEach(x -> {
      if (!ignoreRefs.contains(x.getName())) {
        projectFields.add(x.getName());
        Pair<Integer, RelDataTypeField> tempRef = findFieldWithIndex(filterAlreadyPresetFilesPrel, x.getName());
        projectExpressions.add(rexBuilder.makeInputRef(tempRef.right.getType(), tempRef.left));
      }
    });

    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, projectFields, SqlValidatorUtil.F_SUGGESTER);
    ProjectPrel projectPrel = ProjectPrel.create(getCluster(), filterAlreadyPresetFilesPrel.getTraitSet(), filterAlreadyPresetFilesPrel, projectExpressions, newRowType);

    return projectPrel;
  }

  private Prel addFilterForFilteringPerPartition(Prel manifestScanTf) {
    RexBuilder rexBuilder = getCluster().getRexBuilder();
    Pair<Integer, RelDataTypeField> fieldPair = findFieldWithIndex(manifestScanTf, RecordReader.DATAFILE_PATH);
    RexNode inputRef = rexBuilder.makeInputRef(fieldPair.right.getType(), fieldPair.left);

    //Ignore other paths
    String path = partialRefreshPaths.get(0);
    path = PathUtils.removeTrailingSlash(path);
    path = PathUtils.removeLeadingSlash(path);

    RexNode partialPath = rexBuilder.makeLiteral("%" + path + "/%");
    RexNode isEqualPartitionPath = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, inputRef, partialPath);

    //Only one level allowed. SubDir of path not allowed.
    RexNode subDirNotAllowed = rexBuilder.makeLiteral("%" + path + "/%/%");
    RexNode isEqualPartitionPathSubDir = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, inputRef, subDirNotAllowed);

    RexNode notHasSubDir = rexBuilder.makeCall(SqlStdOperatorTable.NOT, isEqualPartitionPathSubDir);

    RexNode orCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, isEqualPartitionPath, notHasSubDir);
    FilterPrel filterPartialRefreshPath = FilterPrel.create(getCluster(), traitSet, manifestScanTf, orCondition);
    return filterPartialRefreshPath;
  }
}
