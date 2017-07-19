/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

import com.dremio.exec.planner.logical.ConvertibleScan;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;
@Deprecated
public class OldNamespaceTable implements TranslatableTable {

  // we serialize after expansion is done so we no longer need this field
  private final transient SchemaConfig schemaConfig;
  private final StoragePlugin plugin;
  private final DatasetConfig datasetConfig;

  public OldNamespaceTable(StoragePlugin plugin, SchemaConfig schemaConfig, DatasetConfig datasetConfig) {
    this.plugin = plugin;
    this.datasetConfig = datasetConfig;
    this.schemaConfig = schemaConfig;
  }

  public DatasetConfig getDatasetConfig() {
    return datasetConfig;
  }

  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }

  public StoragePlugin getPlugin() {
    return plugin;
  }

  @Override
  public RelNode toRel(ToRelContext toRelContext, RelOptTable relOptTable) {
    return new ConvertibleScan(toRelContext.getCluster(), toRelContext.getCluster().traitSet(), relOptTable, plugin,
        new ConversionContext.NamespaceConversionContext(schemaConfig.getAuthContext(), datasetConfig));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return getBatchSchema().toCalciteRecordType(relDataTypeFactory);
  }

  public BatchSchema getBatchSchema(){
    Preconditions.checkNotNull(datasetConfig, "Dataset was null.");
    return BatchSchema.fromDataset(datasetConfig);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TABLE;
  }
}