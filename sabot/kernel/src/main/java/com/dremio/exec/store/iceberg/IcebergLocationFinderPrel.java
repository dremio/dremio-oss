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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;

/**
 * Iceberg table location finder converts the metadata json into the iceberg table base location.
 * The internal action involves loading the metadata json, and looking for "location" attribute.
 */
public class IcebergLocationFinderPrel extends TableFunctionPrel {
  private final RelNode child;

  public IcebergLocationFinderPrel(
    StoragePluginId storagePluginId,
    RelOptCluster cluster,
    RelTraitSet traitSet,
    RelNode child,
    RelDataType rowType,
    Function<RelMetadataQuery, Double> estimateRowCountFn,
    int survivingRecords,
    String user,
    Map<String, String> tablePropertiesSkipCriteria,
    boolean continueOnError) {
    this(
      cluster,
      traitSet,
      child,
      TableFunctionUtil.getLocationFinderTableFunctionConfig(TableFunctionConfig.FunctionType.ICEBERG_TABLE_LOCATION_FINDER,
        CalciteArrowHelper.fromCalciteRowType(rowType), storagePluginId, tablePropertiesSkipCriteria, continueOnError),
      rowType,
      estimateRowCountFn,
      (long) survivingRecords,
      user);
  }

  private IcebergLocationFinderPrel(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelNode child,
    TableFunctionConfig functionConfig,
    RelDataType rowType,
    Function<RelMetadataQuery, Double> estimateRowCountFn,
    Long survivingRecords,
    String user) {
    super(cluster, traits, null, child, null, functionConfig, rowType, estimateRowCountFn,
      survivingRecords, Collections.emptyList(), user);
    this.child = child;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergLocationFinderPrel(getCluster(), getTraitSet(), sole(inputs), getTableFunctionConfig(),
      getRowType(), getEstimateRowCountFn(), getSurvivingRecords(), user);
  }

  @Override
  protected double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    return child.estimateRowCount(mq); // number of output rows = number of input rows
  }
}
