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
package com.dremio.exec.store.parquet;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

/** Convert scan prel to parquet group scan. */
@Options
public class ParquetScanPrel extends ScanPrelBase implements PruneableScan {

  public static final LongValidator RESERVE =
      new PositiveLongValidator(
          "planner.op.scan.parquet.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT =
      new PositiveLongValidator(
          "planner.op.scan.parquet.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);
  public static final TypeValidators.BooleanValidator C3_RUNTIME_AFFINITY =
      new TypeValidators.BooleanValidator("c3.runtime.affinity", false);

  private final ParquetScanFilter filter;
  private final ParquetScanRowGroupFilter rowGroupFilter;
  private final RelDataType cachedRelDataType;
  private final boolean arrowCachingEnabled;

  public ParquetScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      ParquetScanFilter filter,
      ParquetScanRowGroupFilter rowGroupFilter,
      boolean arrowCachingEnabled,
      List<Info> runtimeFilters) {
    super(
        cluster,
        traitSet,
        table,
        pluginId,
        dataset,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        runtimeFilters);
    this.filter = filter;
    this.rowGroupFilter = rowGroupFilter;
    this.cachedRelDataType = null;
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  // Clone used for copy
  private ParquetScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata dataset,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      ParquetScanFilter filter,
      ParquetScanRowGroupFilter rowGroupFilter,
      RelDataType relDataType,
      boolean arrowCachingEnabled,
      List<Info> runtimeFilters) {
    super(
        cluster,
        traitSet,
        table,
        pluginId,
        dataset,
        projectedColumns,
        observedRowcountAdjustment,
        hints,
        runtimeFilters);
    this.filter = filter;
    this.rowGroupFilter = rowGroupFilter;
    this.cachedRelDataType = relDataType;
    if (relDataType != null) {
      rowType = relDataType;
    }
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  @Override
  public RelDataType deriveRowType() {
    if (cachedRelDataType != null) {
      return cachedRelDataType;
    }
    return super.deriveRowType();
  }

  @Override
  public boolean hasFilter() {
    return filter != null;
  }

  @Override
  public ParquetScanFilter getFilter() {
    return filter;
  }

  public ParquetScanRowGroupFilter getRowGroupFilter() {
    return rowGroupFilter;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final BatchSchema schema =
        cachedRelDataType == null
            ? getTableMetadata().getSchema().maskAndReorder(getProjectedColumns())
            : CalciteArrowHelper.fromCalciteRowType(cachedRelDataType);
    return new ParquetGroupScan(
        creator.props(this, getTableMetadata().getUser(), schema, RESERVE, LIMIT),
        getTableMetadata(),
        getProjectedColumns(),
        filter,
        cachedRelDataType,
        arrowCachingEnabled,
        creator.getContext().getOptions().getOption(C3_RUNTIME_AFFINITY));
  }

  @Override
  public ParquetScanPrel cloneWithProject(List<SchemaPath> projection) {
    return new ParquetScanPrel(
        getCluster(),
        getTraitSet(),
        table,
        pluginId,
        tableMetadata,
        projection,
        observedRowcountAdjustment,
        hints,
        filter != null
            ? filter.applyProjection(projection, rowType, getCluster(), getBatchSchema())
            : filter,
        rowGroupFilter != null
            ? rowGroupFilter.applyProjection(projection, rowType, getCluster(), getBatchSchema())
            : rowGroupFilter,
        arrowCachingEnabled,
        getRuntimeFilters());
  }

  @Override
  public ParquetScanPrel applyDatasetPointer(TableMetadata newDatasetPointer) {
    return new ParquetScanPrel(
        getCluster(),
        traitSet,
        getTable(),
        pluginId,
        newDatasetPointer,
        getProjectedColumns(),
        observedRowcountAdjustment,
        hints,
        filter,
        rowGroupFilter,
        cachedRelDataType,
        arrowCachingEnabled,
        getRuntimeFilters());
  }

  @Override
  public double getFilterReduction() {
    if (filter != null) {
      double selectivity = 0.15d;

      double max =
          PrelUtil.getPlannerSettings(getCluster()).getFilterMaxSelectivityEstimateFactor();
      double min =
          PrelUtil.getPlannerSettings(getCluster()).getFilterMinSelectivityEstimateFactor();

      if (selectivity < min) {
        selectivity = min;
      }
      if (selectivity > max) {
        selectivity = max;
      }

      return selectivity;
    } else {
      return 1d;
    }
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    pw = super.explainTerms(pw);
    return pw.itemIf("filters", filter, filter != null)
        .itemIf("row_group_filter", rowGroupFilter, rowGroupFilter != null);
  }

  @Override
  public double getCostAdjustmentFactor() {
    return filter != null ? filter.getCostAdjustment() : super.getCostAdjustmentFactor();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ParquetScanPrel(
        getCluster(),
        traitSet,
        getTable(),
        pluginId,
        tableMetadata,
        getProjectedColumns(),
        observedRowcountAdjustment,
        hints,
        filter,
        rowGroupFilter,
        cachedRelDataType,
        arrowCachingEnabled,
        getRuntimeFilters());
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }
}
