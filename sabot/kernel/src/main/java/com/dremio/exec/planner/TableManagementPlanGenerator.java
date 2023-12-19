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

import static com.dremio.exec.store.SystemSchemas.DATAFILE_PATH;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_METADATA;

import java.math.BigDecimal;
import java.util.List;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.Prule;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.physical.UnionAllPrel;
import com.dremio.exec.planner.physical.UnionExchangePrel;
import com.dremio.exec.planner.physical.WriterPrule;
import com.dremio.exec.store.OperationType;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import com.dremio.exec.store.iceberg.IcebergSnapshotBasedRefreshPlanBuilder;
import com.dremio.exec.store.iceberg.ManifestContentType;
import com.dremio.exec.store.iceberg.model.ImmutableManifestScanOptions;
import com.dremio.exec.store.iceberg.model.ManifestScanOptions;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Abstract to generate physical plan for DML and OPTIMIZE
 */
public abstract class TableManagementPlanGenerator {

  protected static final List<String> deleteFilesMetadataInputCols = ImmutableList.of(ColumnUtils.ROW_COUNT_COLUMN_NAME, ColumnUtils.FILE_PATH_COLUMN_NAME, ICEBERG_METADATA);

  protected final RelOptTable table;
  protected final RelOptCluster cluster;
  protected final RelTraitSet traitSet;
  protected final RelNode input;
  protected final TableMetadata tableMetadata;
  protected final CreateTableEntry createTableEntry;
  protected final OptimizerRulesContext context;

  public TableManagementPlanGenerator(RelOptTable table, RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                                      TableMetadata tableMetadata, CreateTableEntry createTableEntry, OptimizerRulesContext context) {
    this.table = Preconditions.checkNotNull(table);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata, "TableMetadata cannot be null.");
    this.createTableEntry = Preconditions.checkNotNull(createTableEntry, "CreateTableEntry cannot be null.");
    this.context = Preconditions.checkNotNull(context, "Context cannot be null.");
    this.cluster = cluster;
    this.traitSet = traitSet;
    this.input = input;
  }

  public abstract Prel getPlan();

  /**
   *    WriterCommitterPrel
   *        |
   *        |
   *    UnionAllPrel ---------------------------------------------|
   *        |                                                     |
   *        |                                                     |
   *    WriterPrel                                            TableFunctionPrel (DELETED_FILES_METADATA)
   *        |                                                 this converts a path into required IcebergMetadata blob
   *        |                                                     |
   *    (input from copyOnWriteResultsPlan)                       (deleted data files list from dataFileAggrPlan)
   */
  protected Prel getDataWriterPlan(RelNode copyOnWriteResultsPlan, final RelNode dataFileAggrPlan) {
    return (new WriterPrule()).createWriter(
      copyOnWriteResultsPlan,
      copyOnWriteResultsPlan.getRowType(),
      tableMetadata.getDatasetConfig(),
      createTableEntry,
      manifestWriterPlan -> {
        try {
          return getMetadataWriterPlan(dataFileAggrPlan, manifestWriterPlan);
        } catch (InvalidRelException e) {
          throw new RuntimeException(e);
        }
      });
  }

  /**
   *    UnionAllPrel <------ WriterPrel
   *        |
   *        |
   *    UnionAllPrel ---------------------------------------------|
   *        |                                                     |
   *        |                                                     |
   *    TableFunctionPrel (DELETED_FILES_METADATA)            TableFunctionPrel (DELETED_FILES_METADATA)
   *        |                                                 this converts deleteFile paths into required IcebergMetadata blob
   *        |                                                     |
   *    (deleted data files list from dataFileAggrPlan)       (deleted data files list from deleteFileAggrPlan)
   */
  protected Prel getDataWriterPlan(RelNode copyOnWriteResultsPlan, final Function<RelNode, Prel> metadataWriterFunction) {
    return (new WriterPrule()).createWriter(
      copyOnWriteResultsPlan,
      copyOnWriteResultsPlan.getRowType(),
      tableMetadata.getDatasetConfig(),
      createTableEntry,
      metadataWriterFunction);
  }

  protected Prel getMetadataWriterPlan(RelNode dataFileAggrPlan, RelNode manifestWriterPlan) throws InvalidRelException {
    // Insert a table function that'll pass the path through and set the OperationType
    TableFunctionPrel deletedFilesTableFunctionPrel = getDeleteFilesMetadataTableFunctionPrel(dataFileAggrPlan,
      getProjectedColumns(), TableFunctionUtil.getDeletedFilesMetadataTableFunctionContext(
        OperationType.DELETE_DATAFILE, RecordWriter.SCHEMA, getProjectedColumns(), true));

    final RelTraitSet traits = traitSet.plus(DistributionTrait.SINGLETON).plus(Prel.PHYSICAL);

    PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster);

    // Union the updating of the deleted data's metadata with the rest
    return getUnionPrel(plannerSettings, traits, manifestWriterPlan, deletedFilesTableFunctionPrel);
  }

  protected ImmutableList<SchemaPath> getProjectedColumns() {
    return RecordWriter.SCHEMA.getFields().stream()
      .map(f -> SchemaPath.getSimplePath(f.getName()))
      .collect(ImmutableList.toImmutableList());
  }

  protected TableFunctionPrel getDeleteFilesMetadataTableFunctionPrel(RelNode input, ImmutableList<SchemaPath> projectedCols, TableFunctionContext tableFunctionContext) {
    return new TableFunctionPrel(
      input.getCluster(),
      input.getTraitSet(),
      table,
      input,
      tableMetadata,
      new TableFunctionConfig(
        TableFunctionConfig.FunctionType.DELETED_FILES_METADATA,
        true,
        tableFunctionContext),
      ScanRelBase.getRowTypeFromProjectedColumns(projectedCols,
        RecordWriter.SCHEMA, input.getCluster()));
  }

  protected Prel getUnionPrel(PlannerSettings plannerSettings, RelTraitSet traits, RelNode manifestWriterPlan, RelNode input) throws InvalidRelException {
    return new UnionAllPrel(cluster,
      traits,
      ImmutableList.of(
        fixRoundRobinTraits(manifestWriterPlan, plannerSettings),
        fixRoundRobinTraits(new UnionExchangePrel(cluster, traits, input), plannerSettings)),
      false);
  }

  protected RelNode fixRoundRobinTraits(RelNode input, PlannerSettings plannerSettings) {
    if (plannerSettings.getOptions().getOption(PlannerSettings.ENABLE_UNIONALL_ROUND_ROBIN)) {
      final RelTraitSet traitsChild = cluster.getPlanner().emptyTraitSet()
        .plus(Prel.PHYSICAL)
        .plus(DistributionTrait.ROUND_ROBIN);
      input = Prule.convert(input, PrelUtil.fixTraits(input.getCluster().getPlanner(), traitsChild));
    }
    return input;
  }

  /**
   * Scan the manifests to return the deleted data files.
   *
   *        Project
   *          |
   *          |
   *  IcebergManifestScanPrel
   *          |
   *          |
   * IcebergManifestListPrel
   */
  protected Prel deleteDataFilePlan(IcebergScanPlanBuilder planBuilder, SnapshotDiffContext snapshotDiffContext) {
    boolean includesIcebergPartitionInfo = false;
    ManifestContentType manifestContentType = ManifestContentType.DATA;
    if(snapshotDiffContext.getFilterApplyOptions() != SnapshotDiffContext.FilterApplyOptions.NO_FILTER){
      manifestContentType = ManifestContentType.ALL;
      includesIcebergPartitionInfo = true;
    }
    ManifestScanOptions manifestScanOptions = new ImmutableManifestScanOptions.Builder()
      .setIncludesSplitGen(false)
      .setIncludesIcebergMetadata(true)
      .setIncludesIcebergPartitionInfo(includesIcebergPartitionInfo)
      .setManifestContentType(manifestContentType)
      .build();

    RelNode output;
    if(snapshotDiffContext.getFilterApplyOptions() == SnapshotDiffContext.FilterApplyOptions.NO_FILTER){
      output = planBuilder.hasDeleteFiles() ? buildRemoveSideDataFilePlan()
        : planBuilder.buildManifestRel(manifestScanOptions);
    } else{
      IcebergSnapshotBasedRefreshPlanBuilder icebergSnapshotBasedRefreshPlanBuilder = planBuilder.createIcebergSnapshotBasedRefreshPlanBuilder();
      //planBuilder.hasDeleteFiles() should always return false for reflections
      output = icebergSnapshotBasedRefreshPlanBuilder.buildManifestRel(manifestScanOptions);
      //filter the delete plan to only delete from the affected partitions
      output = icebergSnapshotBasedRefreshPlanBuilder.buildIncrementalReflectionDeleteFilesFilter(output, snapshotDiffContext);
    }
    RexBuilder rexBuilder = cluster.getRexBuilder();

    Pair<Integer, RelDataTypeField> datafilePathCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), DATAFILE_PATH);
    Pair<Integer, RelDataTypeField> icebergMetadataCol = MoreRelOptUtil.findFieldWithIndex(output.getRowType().getFieldList(), ICEBERG_METADATA);

    final List<RexNode> projectExpressions = ImmutableList.of(rexBuilder.makeBigintLiteral(BigDecimal.ONE),
      rexBuilder.makeInputRef(datafilePathCol.right.getType(), datafilePathCol.left),
      rexBuilder.makeInputRef(icebergMetadataCol.right.getType(), icebergMetadataCol.left));

    RelDataType newRowType = RexUtil.createStructType(rexBuilder.getTypeFactory(), projectExpressions, deleteFilesMetadataInputCols, SqlValidatorUtil.F_SUGGESTER);

    return ProjectPrel.create(output.getCluster(), output.getTraitSet(), output, projectExpressions, newRowType);
  }

  protected RelNode buildRemoveSideDataFilePlan() {
    return null;
  }
}
