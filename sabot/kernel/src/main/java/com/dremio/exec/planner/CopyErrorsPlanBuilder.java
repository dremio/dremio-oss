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
package com.dremio.exec.planner;



import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.ExtendedProperties;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.copyinto.CopyErrorsExtendedProperties;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.easy.EasyScanTableFunctionPrel;
import com.dremio.exec.tablefunctions.copyerrors.CopyErrorsCatalogMetadata;

import io.protostuff.ByteString;

/**
 * Expand plans for copy_errors table function
 */
public class CopyErrorsPlanBuilder extends CopyIntoTablePlanBuilderBase {

  private final CopyErrorsCatalogMetadata copyErrorsMetadata;
  public CopyErrorsPlanBuilder(RelOptTable targetTable, RelDataType rowType,  RelOptCluster cluster, RelTraitSet traitSet,
                               TableMetadata tableMetadata, OptimizerRulesContext context,
                               CopyIntoTableContext copyIntoTableContext, CopyErrorsCatalogMetadata copyErrorsMetadata) {
    super(targetTable, rowType, cluster, traitSet, tableMetadata, context, copyIntoTableContext);
    this.copyErrorsMetadata = copyErrorsMetadata;
  }

  /**
   * Construct an {@link ExtendedProperties} object and serialize it to {@link ByteString}. The properties object
   * works as a wrapper around additional reader properties like
   * <ul>
   *   <li>{@link CopyErrorsExtendedProperties}</li>
   * </ul>
   */
  @Override
  protected ByteString getExtendedProperties() {
    ExtendedProperties properties = new ExtendedProperties();
    properties.setProperty(ExtendedProperties.PropertyKey.COPY_ERROR_PROPERTIES,
        new CopyErrorsExtendedProperties(copyIntoTableContext.getOriginalQueryId(), tableMetadata.getSchema()));
    return ExtendedProperties.Util.getByteString(properties);
  }

  @Override
  public Prel buildEasyScanTableFunctionPrel(Prel hashToRandomExchange) {
    TableFunctionConfig easyScanTableFunctionConfig = TableFunctionUtil.getEasyScanTableFunctionConfig(
        tableMetadata,
        null,
        copyErrorsMetadata.getSchema(),
        getSchemaPaths(copyErrorsMetadata.getSchema()),
        format,
        extendedFormatOptions,
        storagePluginId,
        getExtendedProperties());

    setupEasyScanParallelism(easyScanTableFunctionConfig);

    return new EasyScanTableFunctionPrel(cluster,
        traitSet.plus(DistributionTrait.ANY),
        targetTable,
        hashToRandomExchange,
        tableMetadata,
        easyScanTableFunctionConfig,
        copyErrorsMetadata.getRowType(),
        DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE);
  }
}
