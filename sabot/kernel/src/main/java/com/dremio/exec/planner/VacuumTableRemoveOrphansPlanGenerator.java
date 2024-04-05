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

import static com.dremio.exec.planner.VacuumOutputSchema.getRowType;
import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_SIZE;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.METADATA_PATH_SCAN_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.PATH;
import static com.dremio.exec.store.SystemSchemas.PATH_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.RECORDS;
import static com.dremio.exec.store.SystemSchemas.TABLE_LOCATION;
import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.ALL_SNAPSHOTS;
import static com.dremio.io.file.UriSchemes.FILE_SCHEME;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import com.dremio.common.JSONOptions;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.cost.iceberg.IcebergCostEstimates;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashAggPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.StreamAggPrel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.physical.ValuesPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.IcebergOrphanFileDeletePrel;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.DirList;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
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
import org.apache.commons.lang3.StringUtils;

/** Expand plans for VACUUM TABLE REMOVE ORPHAN FILES flow. */
public class VacuumTableRemoveOrphansPlanGenerator extends VacuumPlanGenerator {
  private final String tableLocation; // This table location should have path scheme info.

  public VacuumTableRemoveOrphansPlanGenerator(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<PartitionChunkMetadata> splits,
      IcebergCostEstimates icebergCostEstimates,
      VacuumOptions vacuumOptions,
      StoragePluginId internalStoragePlugin,
      StoragePluginId storagePluginId,
      String user,
      String tableLocation) {
    super(
        cluster,
        traitSet,
        splits,
        icebergCostEstimates,
        vacuumOptions,
        internalStoragePlugin,
        storagePluginId,
        user);
    this.tableLocation = tableLocation;
  }

  /*
   *                        Screen
   *                          ▲
   *                          │
   *                Agg (sum size, number of files)
   *                          ▲
   *                          │
   *                UnionExchange
   *                          ▲
   *                          │
   *                Filter (remove noop records==0)
   *                          ▲
   *                          │
   *                IcebergOrphanDeleteFilesTF
   *                          ▲
   *                          │
   *                Filter (right.path==null)
   *                          ▲
   *                          │
   *                Left Hash Join (path)
   *                 ▲                  ▲
   *                 │                  │
   * Project (path, size)       Project (path, type)
   *          ▲                         ▲
   *          │                         │
   * Filter (mtime > x)         HashAgg (Deduplication)
   *          ▲                         ▲
   *          │                         │
   * HashAgg (Deduplication)    IcebergManifestScanTF
   *          ▲                         ▲
   *          │                         │
   * IcebergLocationFinderTF    IcebregManifestListScanTF
   *          ▲                         ▲
   *          │                         │
   * IcebergCommitScanner(abs)  PartitionStatsScanTF
   *                                    ▲
   *                                    │
   *                            SnapshotsScanPlan(abs)
   */
  @Override
  public Prel buildPlan() {
    try {
      Prel locationProviderPlan = locationProviderPrel();
      Prel allRemovablePathsPlan = listAllWalkableReferencesPlan(locationProviderPlan);
      Prel allLiveRefsPlan = deDupFilePathAndTypeScanPlan(createLiveSnapshotsProducerPlan());
      Prel orphanFilesPlan = orphanFilesPlan(allRemovablePathsPlan, allLiveRefsPlan);
      Prel deleteOrphanFilesPlan = deleteOrphanFilesPlan(orphanFilesPlan);
      return outputSummaryPlan(deleteOrphanFilesPlan);
    } catch (InvalidRelException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Prel projectFilePathAndType(Prel input) {
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_TYPE);
    Pair<Integer, RelDataTypeField> filePathCol =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_PATH);
    Pair<Integer, RelDataTypeField> fileTypeCol =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), FILE_TYPE);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    // If filePathCol contains URI_SCHEME, it needs to trim the scheme; else no trim. Then, if the
    // URI_SCHEME is
    // FILE_SCHEME, i.e., 'file:///path', set the trim index to be 3 and keep the finally trimmed
    // path as '/path'.
    // Other scheme types, S3_SCHEME or 's3://path', trim it as '/path'.

    // NOTE: For SqlStdOperatorTable.POSITION, if it can find the sub string, it returns index
    // greater or equal to 1.
    // Otherwise, not found, it returns index as 0.
    RexInputRef filePathExpr =
        rexBuilder.makeInputRef(filePathCol.right.getType(), filePathCol.left);
    RexNode uriSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION, rexBuilder.makeLiteral("://"), filePathExpr);
    RexNode uriSchemeCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            uriSchemeExpr,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(INTEGER), true));

    RexNode fileSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION,
            rexBuilder.makeLiteral(FILE_SCHEME + "://"),
            filePathExpr);
    RexNode fileSchemeCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            fileSchemeExpr,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(INTEGER), true));

    // If the path contains FILE_SCHEME, i.e., 'file:///path', set the trim index to be 3 and keep
    // the finally trimmed path as '/path'.
    RexNode trimIndexExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            fileSchemeCheck,
            rexBuilder.makeLiteral(3, typeFactory.createSqlType(INTEGER), true),
            rexBuilder.makeLiteral(2, typeFactory.createSqlType(INTEGER), true));

    // The starting index is from the position of finding '://' + 2 or 3 .
    RexNode trimExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, trimIndexExpr, uriSchemeExpr);

    RexNode schemeTrimExpr =
        rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, filePathExpr, trimExpr);
    RexNode filePathTrimExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            uriSchemeCheck,
            schemeTrimExpr, // File path needs to trim UriScheme info.
            filePathExpr); // File path don't need to be trimmed, and keep original path.

    RexNode fileSizeExpr = rexBuilder.makeInputRef(fileTypeCol.right.getType(), fileTypeCol.left);
    final List<RexNode> projectExpressions = ImmutableList.of(filePathTrimExpr, fileSizeExpr);
    RelDataType newRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            projectFields,
            SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(
        input.getCluster(), input.getTraitSet(), input, projectExpressions, newRowType);
  }

  protected Prel createMetadataPathScanPlan() throws InvalidProtocolBufferException {
    IcebergProtobuf.IcebergDatasetSplitXAttr splitXAttrs = getSplitXAttr();
    ObjectNode tableMetadataEntry = OBJECT_MAPPER.createObjectNode();
    tableMetadataEntry.put(METADATA_FILE_PATH, splitXAttrs.getPath());

    return new ValuesPrel(
        cluster,
        traitSet,
        VacuumOutputSchema.getRowType(METADATA_PATH_SCAN_SCHEMA, cluster.getTypeFactory()),
        new JSONOptions(tableMetadataEntry),
        icebergCostEstimates.getTablesCount());
  }

  private IcebergProtobuf.IcebergDatasetSplitXAttr getSplitXAttr()
      throws InvalidProtocolBufferException {
    Preconditions.checkState(
        splits.size() == 1,
        "Expecting single split, which represents a table. Found %s.",
        splits.size());
    PartitionProtobuf.DatasetSplit extendedProp =
        Iterables.getFirst(splits.get(0).getDatasetSplits(), null);
    Preconditions.checkNotNull(extendedProp, "SplitXAttrs not setup correctly");
    return LegacyProtobufSerializer.parseFrom(
        IcebergProtobuf.IcebergDatasetSplitXAttr.PARSER, extendedProp.getSplitExtendedProperty());
  }

  protected Prel createLiveSnapshotsProducerPlan() {
    return snapshotsScanPlan(ALL_SNAPSHOTS);
  }

  @Override
  protected Prel outputSummaryPlan(Prel input) throws InvalidRelException {
    RelOptCluster cluster = input.getCluster();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RelDataType nullableBigInt =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(BIGINT), true);

    RelDataTypeField records = input.getRowType().getField(RECORDS, false, false);
    RexNode removeZeroRecordEntries =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(records.getType(), records.getIndex()),
            rexBuilder.makeZeroLiteral(nullableBigInt));
    Prel filteredPlan =
        FilterPrel.create(cluster, input.getTraitSet(), input, removeZeroRecordEntries);

    final List<String> summaryCols =
        VacuumOutputSchema.REMOVE_ORPHANS_OUTPUT_SCHEMA.getFields().stream()
            .map(Field::getName)
            .collect(Collectors.toList());
    Pair<Integer, RelDataTypeField> fileCountIn =
        MoreRelOptUtil.findFieldWithIndex(filteredPlan.getRowType().getFieldList(), RECORDS);
    Pair<Integer, RelDataTypeField> fileSizeIn =
        MoreRelOptUtil.findFieldWithIndex(filteredPlan.getRowType().getFieldList(), FILE_SIZE);

    RexNode fileCountExpr = rexBuilder.makeInputRef(fileCountIn.right.getType(), fileCountIn.left);
    RexNode fileSizeExpr = rexBuilder.makeInputRef(fileSizeIn.right.getType(), fileSizeIn.left);
    final List<RexNode> projectExpressions = ImmutableList.of(fileCountExpr, fileSizeExpr);
    RelDataType summaryRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            summaryCols,
            SqlValidatorUtil.F_SUGGESTER);
    Prel outputColProject =
        ProjectPrel.create(
            cluster, filteredPlan.getTraitSet(), filteredPlan, projectExpressions, summaryRowType);

    Prel unionExchangePlan =
        new UnionExchangePrel(
            cluster,
            outputColProject.getTraitSet().plus(DistributionTrait.SINGLETON),
            outputColProject);

    List<AggregateCall> aggs =
        summaryCols.stream()
            .map(c -> buildAggregateCall(unionExchangePlan, summaryRowType, c))
            .collect(Collectors.toList());
    Prel agg =
        StreamAggPrel.create(
            cluster,
            unionExchangePlan.getTraitSet(),
            unionExchangePlan,
            ImmutableBitSet.of(),
            Collections.EMPTY_LIST,
            aggs,
            null);

    // Project: return 0 as row count in case there is no Agg record (i.e., no orphan files to
    // delete)
    List<RexNode> projectExprs =
        summaryCols.stream().map(c -> notNullProjectExpr(agg, c)).collect(Collectors.toList());
    RelDataType projectRowType =
        RexUtil.createStructType(
            agg.getCluster().getTypeFactory(), projectExprs, summaryCols, null);
    return ProjectPrel.create(cluster, agg.getTraitSet(), agg, projectExprs, projectRowType);
  }

  private Prel listAllWalkableReferencesPlan(Prel locationProviderPrel) {
    Prel dirListPlan = getDirListingTableFunctionPrel(locationProviderPrel);
    RelDataTypeField timeCutOff =
        dirListPlan.getRowType().getField(DirList.OUTPUT_SCHEMA.MODIFICATION_TIME, false, false);
    Prel filterCutOffPlan = filterCutOff(dirListPlan, timeCutOff);
    Prel filePathsPlan = projectFilePathOnDirList(filterCutOffPlan);
    return filePathsPlan;
  }

  protected Prel locationProviderPrel() {
    ObjectNode dirListingEntry = OBJECT_MAPPER.createObjectNode();
    // Adopt user-input location to list orphan files, if applicable. Otherwise, use table's
    // location itself.
    String dirListingLocation =
        StringUtils.isNoneEmpty(vacuumOptions.getLocation())
            ? vacuumOptions.getLocation()
            : tableLocation;
    dirListingEntry.put(PATH, dirListingLocation);
    return new ValuesPrel(
        cluster,
        traitSet,
        VacuumOutputSchema.getRowType(PATH_SCHEMA, cluster.getTypeFactory()),
        new JSONOptions(dirListingEntry),
        1);
  }

  private Prel projectFilePathOnDirList(Prel dirList) {
    final List<String> projectFields = ImmutableList.of(FILE_PATH, FILE_SIZE);
    Pair<Integer, RelDataTypeField> filePathIn =
        MoreRelOptUtil.findFieldWithIndex(
            dirList.getRowType().getFieldList(), DirList.OUTPUT_SCHEMA.FILE_PATH);
    Pair<Integer, RelDataTypeField> fileSizeIn =
        MoreRelOptUtil.findFieldWithIndex(
            dirList.getRowType().getFieldList(), DirList.OUTPUT_SCHEMA.FILE_SIZE);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    // The file paths listed by DirList could include following cases:
    // 1. has complete scheme path info, e.g., 'hdfs://' + path.
    // 2. has FILE_SCHEME scheme and path separator, e.g., 'file:/' + path.
    // 3. only relative path, e.g., '/' + path.

    // We need to trim scheme info and as the trimmed file path as the style like '/' + path.
    // Otherwise, it can't be filtered by the Join op.

    // Expression to trim complete scheme info.
    RexInputRef filePathExpr = rexBuilder.makeInputRef(filePathIn.right.getType(), filePathIn.left);
    RexNode uriSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION, rexBuilder.makeLiteral("://"), filePathExpr);
    RexNode uriSchemeCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            uriSchemeExpr,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(INTEGER), true));

    RexNode trimCompleteSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.SUBSTRING,
            filePathExpr,
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                rexBuilder.makeLiteral(2, typeFactory.createSqlType(INTEGER), true),
                uriSchemeExpr));

    // Expression to trim 'file:/'
    RexNode fileSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.POSITION, rexBuilder.makeLiteral(":/"), filePathExpr);
    RexNode fileSchemeCheck =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            fileSchemeExpr,
            rexBuilder.makeZeroLiteral(typeFactory.createSqlType(SqlTypeName.INTEGER)));

    RexNode trimFileSchemeExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.SUBSTRING,
            filePathExpr,
            rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                rexBuilder.makeLiteral(1, typeFactory.createSqlType(INTEGER), true),
                fileSchemeExpr));

    // If the path contains SCHEME_SEPARATOR, i.e., '://', trim complete scheme info, e.g.,
    // 'hdfs://',
    // else if the path contains FILE_SCHEME, i.e., 'file:/path', trim 'file:' substring,
    // and finally keep the trimmed path as '/path'.
    RexNode filePathTrimExpr =
        rexBuilder.makeCall(
            SqlStdOperatorTable.CASE,
            uriSchemeCheck,
            trimCompleteSchemeExpr, // Trim complete scheme info
            rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                fileSchemeCheck,
                trimFileSchemeExpr, // Trim FileScheme
                filePathExpr)); // Don't need trim and keep original path string

    RexNode fileSizeExpr = rexBuilder.makeInputRef(fileSizeIn.right.getType(), fileSizeIn.left);
    final List<RexNode> projectExpressions = ImmutableList.of(filePathTrimExpr, fileSizeExpr);
    RelDataType newRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            projectFields,
            SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(
        dirList.getCluster(), dirList.getTraitSet(), dirList, projectExpressions, newRowType);
  }

  private Prel filterCutOff(Prel input, RelDataTypeField modificationTimeField) {
    RexBuilder rexBuilder = cluster.getRexBuilder();

    RexNode cutOffCondition =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            rexBuilder.makeInputRef(
                modificationTimeField.getType(), modificationTimeField.getIndex()),
            rexBuilder.makeLiteral(recentFileSelectionCutOff(), modificationTimeField.getType()));

    return FilterPrel.create(input.getCluster(), input.getTraitSet(), input, cutOffCondition);
  }

  protected long recentFileSelectionCutOff() {
    return vacuumOptions.getOlderThanInMillis();
  }

  private Prel getDirListingTableFunctionPrel(Prel input) {
    BatchSchema dirListingSchema = DirList.OUTPUT_SCHEMA.BATCH_SCHEMA;
    TableFunctionConfig dirListingConfig =
        TableFunctionUtil.getDirListingTableFunctionConfig(storagePluginId, dirListingSchema);

    Function<RelMetadataQuery, Double> estimateRowCountFn =
        mq -> (double) icebergCostEstimates.getEstimatedRows();
    TableFunctionPrel dirListingPrel =
        new TableFunctionPrel(
            cluster,
            input.getTraitSet(),
            input,
            dirListingConfig,
            getRowType(dirListingSchema, cluster.getTypeFactory()),
            estimateRowCountFn,
            icebergCostEstimates.getEstimatedRows(),
            user);

    return dirListingPrel;
  }

  protected Prel projectPath(Prel input) {
    Pair<Integer, RelDataTypeField> tableLocationCol =
        MoreRelOptUtil.findFieldWithIndex(input.getRowType().getFieldList(), TABLE_LOCATION);
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode tableLocationExpr =
        rexBuilder.makeInputRef(tableLocationCol.right.getType(), tableLocationCol.left);

    final List<RexNode> projectExpressions = ImmutableList.of(tableLocationExpr);
    RelDataType newRowType =
        RexUtil.createStructType(
            rexBuilder.getTypeFactory(),
            projectExpressions,
            ImmutableList.of(PATH),
            SqlValidatorUtil.F_SUGGESTER);
    return ProjectPrel.create(
        input.getCluster(), input.getTraitSet(), input, projectExpressions, newRowType);
  }

  protected Prel reduceDuplicateFilePaths(Prel input) {
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
            SystemSchemas.TABLE_LOCATION);

    ImmutableBitSet groupSet =
        ImmutableBitSet.of(input.getRowType().getField(TABLE_LOCATION, false, false).getIndex());
    try {
      return HashAggPrel.create(
          input.getCluster(),
          input.getTraitSet(),
          input,
          groupSet,
          ImmutableList.of(groupSet),
          ImmutableList.of(aggOnFilePath),
          null);
    } catch (InvalidRelException e) {
      throw new RuntimeException("Failed to create HashAggPrel during delete file scan.", e);
    }
  }

  @Override
  protected Prel deleteOrphanFilesPlan(Prel input) {
    BatchSchema outSchema =
        BatchSchema.newBuilder()
            .addField(Field.nullable(FILE_PATH, Types.MinorType.VARCHAR.getType()))
            .addField(Field.nullable(FILE_SIZE, Types.MinorType.BIGINT.getType()))
            .addField(Field.nullable(RECORDS, Types.MinorType.BIGINT.getType()))
            .setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE)
            .build();

    // We do overestimate instead of underestimate. 1) Use file counts from ALL snapshot; 2)
    // consider every snapshot has partition stats files.
    return new IcebergOrphanFileDeletePrel(
        storagePluginId,
        input.getCluster(),
        input.getTraitSet(),
        outSchema,
        input,
        icebergCostEstimates.getEstimatedRows(),
        user,
        tableLocation);
  }

  @Override
  protected boolean enableCarryForwardOnPartitionStats() {
    return true;
  }

  protected boolean continueOnError() {
    return false;
  }

  @Override
  protected RexNode notNullProjectExpr(Prel input, String fieldName) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory typeFactory = cluster.getTypeFactory();

    final RexNode zeroLiteral =
        rexBuilder.makeLiteral(0, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
    RelDataTypeField field = input.getRowType().getField(fieldName, false, false);
    RexInputRef inputRef = rexBuilder.makeInputRef(field.getType(), field.getIndex());
    RexNode rowCountRecordExistsCheckCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);

    RexNode notNullRexNode =
        fieldName.equals(VacuumOutputSchema.DELETED_FILES_SIZE_MB)
            ? rexBuilder.makeCall(
                SqlStdOperatorTable.DIVIDE,
                inputRef,
                rexBuilder.makeLiteral(
                    1024 * 1024L, typeFactory.createSqlType(SqlTypeName.INTEGER), true))
            : inputRef;

    // case when the count of row count records is 0, return 0, else return aggregated row count
    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE, rowCountRecordExistsCheckCondition, zeroLiteral, notNullRexNode);
  }
}
