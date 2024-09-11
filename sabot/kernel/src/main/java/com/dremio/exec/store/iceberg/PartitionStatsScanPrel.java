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

import static com.dremio.exec.store.SystemSchemas.FILE_PATH;
import static com.dremio.exec.store.SystemSchemas.FILE_TYPE;
import static com.dremio.exec.store.SystemSchemas.METADATA_FILE_PATH;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.CarryForwardAwareTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/** A prel for PartitionStatsScanTableFunction */
public class PartitionStatsScanPrel extends TableFunctionPrel {
  private StoragePluginId storagePluginId;

  public PartitionStatsScanPrel(
      StoragePluginId storagePluginId,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      BatchSchema schema,
      Long survivingRecords,
      String user,
      boolean isCarryForwardEnabled,
      String schemeVariate) {
    this(
        storagePluginId,
        cluster,
        traitSet,
        child,
        getTableFunctionConfig(schema, storagePluginId, isCarryForwardEnabled, schemeVariate),
        CalciteArrowHelper.wrap(schema).toCalciteRecordType(cluster.getTypeFactory(), true),
        survivingRecords,
        user);
    this.storagePluginId = storagePluginId;
  }

  private PartitionStatsScanPrel(
      StoragePluginId storagePluginId,
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      TableFunctionConfig functionConfig,
      RelDataType rowType,
      long survivingRecords,
      String user) {
    super(
        cluster,
        traits,
        null,
        child,
        null,
        functionConfig,
        rowType,
        null,
        survivingRecords,
        Collections.emptyList(),
        user);
    this.storagePluginId = storagePluginId;
  }

  private static TableFunctionConfig getTableFunctionConfig(
      BatchSchema schema,
      StoragePluginId storagePluginId,
      boolean isCarryForwardEnabled,
      String schemeVariate) {
    TableFunctionContext tableFunctionContext =
        new CarryForwardAwareTableFunctionContext(
            schema,
            storagePluginId,
            isCarryForwardEnabled,
            ImmutableMap.of(
                SchemaPath.getSimplePath(METADATA_FILE_PATH), SchemaPath.getSimplePath(FILE_PATH)),
            FILE_TYPE,
            IcebergFileType.METADATA_JSON.name(),
            schemeVariate);
    return new TableFunctionConfig(
        TableFunctionConfig.FunctionType.ICEBERG_PARTITION_STATS_SCAN, true, tableFunctionContext);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PartitionStatsScanPrel(
        storagePluginId,
        getCluster(),
        getTraitSet(),
        sole(inputs),
        getTableFunctionConfig(),
        getRowType(),
        getSurvivingRecords(),
        user);
  }

  @Override
  protected double defaultEstimateRowCount(
      TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    return (double) getSurvivingRecords();
  }
}
