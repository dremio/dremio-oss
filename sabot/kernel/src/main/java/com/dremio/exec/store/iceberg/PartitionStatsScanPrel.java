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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;

/**
 * A prel for PartitionStatsScanTableFunction
 */
public class PartitionStatsScanPrel extends TableFunctionPrel {
  public PartitionStatsScanPrel(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    RelOptTable table,
    RelNode child,
    BatchSchema schema,
    TableMetadata tableMetadata,
    Long survivingRecords) {
    this(
      cluster,
      traitSet,
      table,
      child,
      tableMetadata,
      TableFunctionUtil.getIcebergPartitionStatsFunctionConfig(schema, tableMetadata),
      CalciteArrowHelper.wrap(schema)
        .toCalciteRecordType(cluster.getTypeFactory(),
          PrelUtil.getPlannerSettings(cluster).isFullNestedSchemaSupport()),
      survivingRecords);
  }

  private PartitionStatsScanPrel(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    RelOptTable table,
    RelNode child,
    TableMetadata tableMetadata,
    TableFunctionConfig functionConfig,
    RelDataType rowType,
    Long survivingRecords) {
    super(cluster, traitSet, table, child, tableMetadata, functionConfig, rowType, survivingRecords);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PartitionStatsScanPrel(getCluster(), getTraitSet(), getTable(), sole(inputs),
      getTableMetadata(), getTableFunctionConfig(), getRowType(), getSurvivingRecords());
  }

  @Override
  protected double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    return (double) getSurvivingRecords();
  }
}
