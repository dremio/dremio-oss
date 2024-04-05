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

import static com.dremio.exec.planner.physical.PlannerSettings.ORPHAN_FILE_DELETE_RECORDS_PER_THREAD;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/** A prel for IcebergOrphanFileDeleteTableFunction */
public class IcebergOrphanFileDeletePrel extends TableFunctionPrel {
  public IcebergOrphanFileDeletePrel(
      StoragePluginId storagePluginId,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      BatchSchema outSchema,
      RelNode child,
      Long survivingRecords,
      String user,
      String tableLocation) {
    this(
        cluster,
        traitSet,
        child,
        TableFunctionUtil.getOrphanFileDeleteTableFunctionConfig(
            outSchema, storagePluginId, tableLocation),
        CalciteArrowHelper.wrap(outSchema)
            .toCalciteRecordType(
                cluster.getTypeFactory(),
                PrelUtil.getPlannerSettings(cluster).isFullNestedSchemaSupport()),
        survivingRecords,
        user);
  }

  private IcebergOrphanFileDeletePrel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      TableFunctionConfig functionConfig,
      RelDataType rowType,
      Long survivingRecords,
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
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergOrphanFileDeletePrel(
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
    // It needs to amplify the estimate row count in order to multiple threads.
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
    double rowMultiplier =
        ((double) plannerSettings.getSliceTarget()
            / plannerSettings.getOptions().getOption(ORPHAN_FILE_DELETE_RECORDS_PER_THREAD));
    return Math.max(getSurvivingRecords() * rowMultiplier, 1);
  }
}
