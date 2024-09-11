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

import static com.dremio.exec.planner.physical.PlannerSettings.ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.record.BatchSchema;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Prel for the Iceberg manifest list scan table function. This supports both data and delete
 * manifest scans.
 */
public class IcebergManifestListScanPrel extends TableFunctionPrel {

  public IcebergManifestListScanPrel(
      StoragePluginId storagePluginId,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      BatchSchema schema,
      List<SchemaPath> projectedColumns,
      Long survivingRecords,
      String user,
      String schemeVariate) {
    this(
        cluster,
        traitSet,
        child,
        TableFunctionUtil.getManifestListScanTableFunctionConfig(
            schema, storagePluginId, schemeVariate),
        ScanRelBase.getRowTypeFromProjectedColumns(projectedColumns, schema, cluster),
        survivingRecords,
        user);
  }

  protected IcebergManifestListScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      TableFunctionConfig functionConfig,
      RelDataType rowType,
      Long survivingRecords,
      String user) {
    super(
        cluster,
        traitSet,
        null,
        child,
        null,
        functionConfig,
        rowType,
        null,
        survivingRecords,
        Collections.emptyList(),
        user);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergManifestListScanPrel(
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
    if (getSurvivingRecords() == null) {
      // we should always have a surviving records count provided which would be based on the data
      // file count from table
      // metadata, but if not make a guess based on config
      final PlannerSettings plannerSettings =
          PrelUtil.getPlannerSettings(getCluster().getPlanner());
      double rowMultiplier =
          ((double) plannerSettings.getSliceTarget()
              / plannerSettings.getOptions().getOption(ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD));
      return Math.max(mq.getRowCount(input) * rowMultiplier, 1);
    }

    return (double) getSurvivingRecords();
  }
}
