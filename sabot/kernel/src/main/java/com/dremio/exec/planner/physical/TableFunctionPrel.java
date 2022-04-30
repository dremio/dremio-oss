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
package com.dremio.exec.planner.physical;

import static com.dremio.exec.planner.physical.PlannerSettings.ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.SplitGenManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.collect.ImmutableList;

/**
 * Prel for TableFunction operator
 */
@Options
public class TableFunctionPrel extends SinglePrel implements RuntimeFilteredRel {

  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  protected final TableMetadata tableMetadata;
  private final ImmutableList<SchemaPath> projectedColumns;
  private final TableFunctionConfig functionConfig;
  private final RelOptTable table;
  private final Long survivingRecords;
  final Function<RelMetadataQuery, Double> estimateRowCountFn;
  private List<RuntimeFilteredRel.Info> runtimeFilters ;

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType) {
    this(cluster, traits, table, child, tableMetadata, projectedColumns, functionConfig, rowType,
      null, null, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Long survivingRecords) {
    this(cluster, traits, table, child, tableMetadata, projectedColumns, functionConfig, rowType, null, survivingRecords, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Function<RelMetadataQuery, Double> estimateRowCountFn) {
    this(cluster, traits, table, child, tableMetadata, projectedColumns, functionConfig, rowType,
      estimateRowCountFn, null, ImmutableList.of());
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Function<RelMetadataQuery, Double> estimateRowCountFn,
                           Long survivingRecords,
                           List<RuntimeFilteredRel.Info> runtimeFilteredRels) {
    super(cluster, traits, child);
    this.tableMetadata = tableMetadata;
    this.projectedColumns = projectedColumns;
    this.functionConfig = functionConfig;
    this.rowType = rowType;
    this.estimateRowCountFn = estimateRowCountFn == null ?
            mq -> defaultEstimateRowCount(functionConfig, mq) : estimateRowCountFn;
    this.table = table;
    this.survivingRecords = survivingRecords;
    this.runtimeFilters = runtimeFilteredRels;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    return new TableFunctionPOP(
      creator.props(this, tableMetadata != null ? tableMetadata.getUser() : null,
        functionConfig.getOutputSchema(), RESERVE, LIMIT),
      childPOP, functionConfig);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableFunctionPrel(getCluster(), traitSet, table, sole(inputs), tableMetadata, projectedColumns,
            functionConfig, rowType, estimateRowCountFn, survivingRecords, runtimeFilters);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if (functionConfig.getFunctionContext().getScanFilter() != null) {
      pw.item("filters", functionConfig.getFunctionContext().getScanFilter().toString());
    }
    if(functionConfig.getFunctionContext().getColumns() != null){
      pw.item("columns",
        functionConfig.getFunctionContext().getColumns().stream()
          .map(Object::toString)
          .collect(Collectors.joining(", ")));
    }
    if(!runtimeFilters.isEmpty()) {
      pw.item("runtimeFilters", runtimeFilters.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ")));
    }
    return explainTableFunction(pw);
  }

  public RelWriter explainTableFunction(RelWriter pw) {
    switch (functionConfig.getType()) {
      case DATA_FILE_SCAN:
        pw.item("table", PathUtils.constructFullPath(functionConfig.getFunctionContext().getTablePath().get(0)));
        break;
      case SPLIT_GEN_MANIFEST_SCAN:
        Expression icebergAnyColExpression;
        try {
          icebergAnyColExpression = IcebergSerDe.deserializeFromByteArray(((SplitGenManifestScanTableFunctionContext) functionConfig.getFunctionContext()).getIcebergAnyColExpression());
        } catch (IOException e) {
          throw new RuntimeIOException(e, "failed to deserialize ManifestFile Filter AnyColExpression");
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("failed to deserialize ManifestFile Filter AnyColExpression" , e);
        }
        if(icebergAnyColExpression != null) {
          pw.item("ManifestFile Filter AnyColExpression", icebergAnyColExpression.toString());
        }
        break;
    }
    return pw;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return estimateRowCountFn.apply(mq);
  }

  private double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    switch (functionConfig.getType()) {
      case FOOTER_READER:
        return DremioCost.LARGE_ROW_COUNT;
      case SPLIT_GENERATION:
        // TODO: This should estimate the number of splits that generated
        break;
      case DATA_FILE_SCAN:
        double selectivityEstimateFactor = 1.0;
        if (functionConfig.getFunctionContext().getScanFilter() != null) {
          final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
          selectivityEstimateFactor = plannerSettings.getFilterMinSelectivityEstimateFactor();
        }
        return survivingDataFileRecords() * selectivityEstimateFactor;
      case SPLIT_GEN_MANIFEST_SCAN:
      case METADATA_REFRESH_MANIFEST_SCAN:
        final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
        double sliceTarget = ((double)plannerSettings.getSliceTarget() /
                plannerSettings.getOptions().getOption(ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD));
        if (tableMetadata.getReadDefinition().getManifestScanStats() == null) {
          return Math.max(mq.getRowCount(input) * sliceTarget, 1);
        }
        return Math.max(tableMetadata.getReadDefinition().getManifestScanStats().getRecordCount() * sliceTarget, 1);
      case SPLIT_ASSIGNMENT:
        return mq.getRowCount(this.input);
    }

    return 1;
  }

  private long survivingDataFileRecords() {
    return survivingRecords == null ? tableMetadata.getApproximateRecordCount() : survivingRecords;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitTableFunction(this, value);
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public boolean isDataScan() {
    return TableFunctionConfig.FunctionType.DATA_FILE_SCAN.equals(functionConfig.getType());
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  @Override
  public List<RuntimeFilteredRel.Info> getRuntimeFilters() {
    return runtimeFilters;
  }

  @Override
  public void addRuntimeFilter(RuntimeFilteredRel.Info filterInfo) {
    runtimeFilters = ImmutableList.<Info>builder()
      .addAll(runtimeFilters)
      .add(filterInfo)
      .build();
    recomputeDigest();
  }
}
