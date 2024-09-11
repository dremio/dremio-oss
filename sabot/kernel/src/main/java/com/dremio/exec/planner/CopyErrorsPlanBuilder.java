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

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.physical.config.CopyIntoExtendedProperties;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.physical.config.copyinto.CopyIntoHistoryExtendedProperties;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.easy.EasyScanTableFunctionPrel;
import com.dremio.exec.tablefunctions.copyerrors.CopyErrorsCatalogMetadata;
import com.dremio.exec.util.ColumnUtils;
import io.protostuff.ByteString;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

/** Expand plans for copy_errors table function */
public class CopyErrorsPlanBuilder extends CopyIntoTablePlanBuilderBase {

  private final CopyErrorsCatalogMetadata copyErrorsMetadata;

  public CopyErrorsPlanBuilder(
      RelOptTable targetTable,
      RelDataType rowType,
      RelOptCluster cluster,
      RelTraitSet traitSet,
      TableMetadata tableMetadata,
      OptimizerRulesContext context,
      CopyIntoTableContext copyIntoTableContext,
      CopyErrorsCatalogMetadata copyErrorsMetadata) {
    // TODO: add support for transformations within copy_errors()
    super(
        targetTable,
        null,
        rowType,
        null,
        cluster,
        traitSet,
        tableMetadata,
        context,
        copyIntoTableContext);
    this.copyErrorsMetadata = copyErrorsMetadata;
  }

  /**
   * Construct an {@link CopyIntoExtendedProperties} object and serialize it to {@link ByteString}.
   * The properties object works as a wrapper around additional reader properties like
   *
   * <ul>
   *   <li>{@link CopyIntoHistoryExtendedProperties}
   * </ul>
   *
   * Marking the target table schema for validation purposes. It is extended by the copy history
   * column for tracking record level detailed errors during COPY_ERRORS()
   */
  @Override
  protected ByteString getExtendedProperties() {
    CopyIntoExtendedProperties properties = new CopyIntoExtendedProperties();
    properties.setProperty(
        CopyIntoExtendedProperties.PropertyKey.COPY_INTO_HISTORY_PROPERTIES,
        new CopyIntoHistoryExtendedProperties(
            copyIntoTableContext.getOriginalQueryId(),
            tableMetadata
                .getSchema()
                .addColumn(
                    Field.nullable(
                        ColumnUtils.COPY_HISTORY_COLUMN_NAME, ArrowType.Utf8.INSTANCE))));
    return CopyIntoExtendedProperties.Util.getByteString(properties);
  }

  @Override
  public Prel buildEasyScanTableFunctionPrel(Prel hashToRandomExchange) {
    TableFunctionConfig easyScanTableFunctionConfig =
        TableFunctionUtil.getEasyScanTableFunctionConfig(
            tableMetadata,
            null,
            copyErrorsMetadata.getSchema(),
            SchemaUtilities.allColPaths(copyErrorsMetadata.getSchema()),
            format,
            extendedFormatOptions,
            storagePluginId,
            getExtendedProperties());

    setupEasyScanParallelism(easyScanTableFunctionConfig);

    return new EasyScanTableFunctionPrel(
        cluster,
        traitSet.plus(DistributionTrait.ANY),
        targetTable,
        hashToRandomExchange,
        tableMetadata,
        easyScanTableFunctionConfig,
        copyErrorsMetadata.getRowType(),
        DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE);
  }

  @Override
  public Prel buildParquetScanTableFunctionPrel(Prel hashToRandomExchange) {

    TableFunctionContext scanTableFunctionContext =
        TableFunctionUtil.getDataFileScanTableFunctionContextForCopyInto(
            format,
            copyErrorsMetadata.getSchema(),
            sourceLocationNSKey,
            storagePluginId,
            SchemaUtilities.allColPaths(copyErrorsMetadata.getSchema()),
            getExtendedProperties());

    TableFunctionConfig scanTableFunctionConfig =
        TableFunctionUtil.getDataFileScanTableFunctionConfig(scanTableFunctionContext, false, 1);

    return new TableFunctionPrel(
        cluster,
        traitSet.plus(DistributionTrait.ANY),
        targetTable,
        hashToRandomExchange,
        tableMetadata,
        scanTableFunctionConfig,
        copyErrorsMetadata.getRowType(),
        DEFAULT_EASY_SCAN_ROW_COUNT_ESTIMATE);
  }
}
