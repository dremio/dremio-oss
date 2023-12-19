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

import java.util.List;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.physical.config.IncrementalRefreshJoinKeyTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.store.TableMetadata;

public final class IcebergIncrementalRefreshJoinKeyPrel extends TableFunctionPrel {
  public IcebergIncrementalRefreshJoinKeyPrel(final RelOptCluster cluster,
                                              final RelTraitSet traits,
                                              final RelOptTable table,
                                              final RelNode child,
                                              final TableMetadata tableMetadata,
                                              final TableFunctionConfig functionConfig,
                                              final RelDataType rowType,
                                              final Function<RelMetadataQuery, Double> estimateRowCountFn,
                                              final Long survivingRecords,
                                              final List<RuntimeFilteredRel.Info> runtimeFilteredRels,
                                              final String user) {
    super(cluster, traits, table, child, tableMetadata, functionConfig, rowType,
      estimateRowCountFn, survivingRecords, runtimeFilteredRels, user);
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
    return new IcebergIncrementalRefreshJoinKeyPrel(getCluster(), getTraitSet(), getTable(), sole(inputs), getTableMetadata(),
        getTableFunctionConfig(), getRowType(), getEstimateRowCountFn(), getSurvivingRecords(),getRuntimeFilters(), user);
  }

  @Override
  public RelWriter explainTableFunction(final RelWriter pw) {
    final IncrementalRefreshJoinKeyTableFunctionContext functionContext = (IncrementalRefreshJoinKeyTableFunctionContext) getTableFunctionConfig().getFunctionContext();
    final String icebergSchema = ((IncrementalRefreshJoinKeyTableFunctionContext) getTableFunctionConfig().getFunctionContext()).getIcebergSchema();
    final PartitionSpec partitionSpec = IcebergSerDe.deserializePartitionSpec(IcebergSerDe.deserializedJsonAsSchema(icebergSchema), functionContext.getPartitionSpec().toByteArray());
    pw.item("join key", IncrementalReflectionByPartitionUtils.toDisplayString(partitionSpec));
    return pw;
  }

  @Override
  protected double defaultEstimateRowCount(final TableFunctionConfig functionConfig, final RelMetadataQuery mq) {
    return (double) getSurvivingRecords();
  }
}
