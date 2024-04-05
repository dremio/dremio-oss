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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/** A prel for IcebergDmlMergeDuplicateCheckTableFunction. */
public class IcebergDmlMergeDuplicateCheckPrel extends TableFunctionPrel {

  public IcebergDmlMergeDuplicateCheckPrel(
      RelNode child, RelOptTable table, TableMetadata tableMetadata) {
    this(
        child.getCluster(),
        child.getTraitSet(),
        table,
        child,
        tableMetadata,
        CalciteArrowHelper.fromCalciteRowType(child.getRowType()));
  }

  private IcebergDmlMergeDuplicateCheckPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      RelNode child,
      TableMetadata tableMetadata,
      BatchSchema batchSchema) {
    this(
        cluster,
        traitSet,
        table,
        child,
        tableMetadata,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_DML_MERGE_DUPLICATE_CHECK,
            true,
            new TableFunctionContext(
                batchSchema,
                batchSchema.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName()))
                    .collect(ImmutableList.toImmutableList()))),
        child.getRowType());
  }

  private IcebergDmlMergeDuplicateCheckPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      RelNode child,
      TableMetadata tableMetadata,
      TableFunctionConfig functionConfig,
      RelDataType rowType) {
    super(cluster, traitSet, table, child, tableMetadata, functionConfig, rowType);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergDmlMergeDuplicateCheckPrel(
        getCluster(),
        getTraitSet(),
        getTable(),
        sole(inputs),
        getTableMetadata(),
        getTableFunctionConfig(),
        getRowType());
  }

  @Override
  protected double defaultEstimateRowCount(TableFunctionConfig ignored, RelMetadataQuery mq) {
    return mq.getRowCount(input);
  }
}
