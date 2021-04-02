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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.collect.ImmutableList;

/**
 * Prel for TableFunction operator
 */
@Options
public class TableFunctionPrel extends SinglePrel{

  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.tablefunction.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  protected final TableMetadata tableMetadata;
  private final ImmutableList<SchemaPath> projectedColumns;
  private final TableFunctionConfig functionConfig;
  private final RelOptTable table;
  final Function<RelMetadataQuery, Double> estimateRowCountFn;

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType) {
    this(cluster, traits, table, child, tableMetadata, projectedColumns, functionConfig, rowType,
            mq -> TableFunctionPrel.defaultEstimateRowCount(functionConfig, mq));
  }

  public TableFunctionPrel(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, RelNode child,
                           TableMetadata tableMetadata,
                           ImmutableList<SchemaPath> projectedColumns,
                           TableFunctionConfig functionConfig,
                           RelDataType rowType,
                           Function<RelMetadataQuery, Double> estimateRowCountFn) {
    super(cluster, traits, child);
    this.tableMetadata = tableMetadata;
    this.projectedColumns = projectedColumns;
    this.functionConfig = functionConfig;
    this.rowType = rowType;
    this.estimateRowCountFn = estimateRowCountFn;
    this.table = table;
  }


  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    return new TableFunctionPOP(
      creator.props(this, tableMetadata.getUser(), functionConfig.getOutputSchema(), RESERVE, LIMIT),
      childPOP, functionConfig);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new TableFunctionPrel(getCluster(), traitSet, table, sole(inputs), tableMetadata, projectedColumns,
            functionConfig, rowType, estimateRowCountFn);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    if(functionConfig.getFunctionContext().getConditions() != null){
      return pw.item("filters",  functionConfig.getFunctionContext().getConditions());
    }
    return pw;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return estimateRowCountFn.apply(mq);
  }

  private static double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    switch (functionConfig.getType()) {
      case SPLIT_GENERATION:
        // TODO: This should estimate the number of splits that generated
      case PARQUET_DATA_SCAN:
        // TODO: This should estimate the row count from the table scan
        // This should read the partition stats and estimate
      case ICEBERG_MANIFEST_SCAN:
        // TODO: This should estimate the number of data files that would be
        // This can read the partition stats and return the file count
    }

    return 1;
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
    return TableFunctionConfig.FunctionType.PARQUET_DATA_SCAN.equals(functionConfig.getType());
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }
}
