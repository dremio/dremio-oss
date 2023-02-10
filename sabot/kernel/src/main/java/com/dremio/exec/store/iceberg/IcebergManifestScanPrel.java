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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.expressions.Expression;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.planner.physical.TableFunctionUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.util.LongRange;

/**
 * Prel for the Iceberg manifest scan table function.  This supports both data and delete manifest scans.
 */
public class IcebergManifestScanPrel extends TableFunctionPrel {

  public IcebergManifestScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      RelNode child,
      TableMetadata tableMetadata,
      BatchSchema schema,
      List<SchemaPath> projectedColumns,
      ManifestScanFilters manifestScanFilters,
      Long survivingRecords,
      ManifestContent manifestContent) {
    this(
        cluster,
        traitSet,
        table,
        child,
        tableMetadata,
        TableFunctionUtil.getManifestScanTableFunctionConfig(tableMetadata, projectedColumns, schema, null,
            manifestContent, manifestScanFilters),
        ScanRelBase.getRowTypeFromProjectedColumns(projectedColumns, schema, cluster),
        survivingRecords);
  }

  protected IcebergManifestScanPrel(
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
    return new IcebergManifestScanPrel(getCluster(), getTraitSet(), getTable(), sole(inputs), getTableMetadata(),
        getTableFunctionConfig(), getRowType(), getSurvivingRecords());
  }

  @Override
  public RelWriter explainTableFunction(RelWriter pw) {
    ManifestScanTableFunctionContext context =
        getTableFunctionConfig().getFunctionContext(ManifestScanTableFunctionContext.class);
    ManifestScanFilters manifestScanFilters = context.getManifestScanFilters();

    if (manifestScanFilters.doesIcebergAnyColExpressionExists()) {
      Expression icebergAnyColExpression = manifestScanFilters.getIcebergAnyColExpressionDeserialized();
      pw.item("ManifestFile Filter AnyColExpression", icebergAnyColExpression.toString());
    }

    if (manifestScanFilters.doesMinPartitionSpecIdExist()) {
      pw.item("partition_spec_id <", manifestScanFilters.getMinPartitionSpecId());
    }

    if (manifestScanFilters.doesSkipDataFileSizeRangeExist()) {
      LongRange range = manifestScanFilters.getSkipDataFileSizeRange();
      pw.item("data_file.file_size_in_bytes between", range);
    }
    pw.item("manifestContent", context.getManifestContent());

    return pw;
  }

  @Override
  protected double defaultEstimateRowCount(TableFunctionConfig functionConfig, RelMetadataQuery mq) {
    if (getSurvivingRecords() == null) {
      // we should always have a surviving records count provided which would be based on the data file count from table
      // metadata, but if not make a guess based on config
      final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
      double rowMultiplier = ((double) plannerSettings.getSliceTarget() /
          plannerSettings.getOptions().getOption(ICEBERG_MANIFEST_SCAN_RECORDS_PER_THREAD));
      return Math.max(mq.getRowCount(input) * rowMultiplier, 1);
    }

    return (double) getSurvivingRecords();
  }
}
