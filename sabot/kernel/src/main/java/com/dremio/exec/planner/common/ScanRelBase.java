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
package com.dremio.exec.planner.common;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.sql.SqlExplainLevel;

/** Base class for all scans implemented in Dremio. Should not be used to match rules. */
public abstract class ScanRelBase extends TableScan {
  private static final int MAX_COLUMNS = 1000;

  public static final double DEFAULT_COST_ADJUSTMENT = 1.0d;

  protected final ImmutableList<SchemaPath> projectedColumns;
  protected final TableMetadata tableMetadata;
  protected final StoragePluginId pluginId;
  protected final double observedRowcountAdjustment;
  protected SnapshotDiffContext snapshotDiffContext = SnapshotDiffContext.NO_SNAPSHOT_DIFF;

  public ScanRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints) {
    super(cluster, traitSet, hints, table);
    this.pluginId = Preconditions.checkNotNull(pluginId);
    this.tableMetadata = Preconditions.checkNotNull(tableMetadata);
    Preconditions.checkArgument(
        observedRowcountAdjustment >= 0 && observedRowcountAdjustment <= 1,
        "observedRowcountAdjustment cannot be set to " + observedRowcountAdjustment);
    this.observedRowcountAdjustment = observedRowcountAdjustment;
    this.projectedColumns =
        projectedColumns != null ? ImmutableList.copyOf(projectedColumns) : getAllColumns(table);
    setProjectedRowType(projectedColumns);
  }

  public ScanRelBase(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      StoragePluginId pluginId,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      List<RelHint> hints,
      SnapshotDiffContext snapshotDiffContext) {
    this(
        cluster,
        traitSet,
        table,
        pluginId,
        tableMetadata,
        projectedColumns,
        observedRowcountAdjustment,
        hints);
    this.snapshotDiffContext = Preconditions.checkNotNull(snapshotDiffContext);
  }

  private static ImmutableList<SchemaPath> getAllColumns(RelOptTable table) {
    return FluentIterable.from(table.getRowType().getFieldNames())
        .transform(
            new Function<String, SchemaPath>() {

              @Override
              public SchemaPath apply(String input) {
                return SchemaPath.getSimplePath(input);
              }
            })
        .toList();
  }

  /**
   * Hive can't handle us returning an ImmutableList, so we add a version of getHints() that has a
   * return type of List<RelHint>.
   *
   * @return The hints member as type List<RelHint>.
   */
  public List<RelHint> getHintsAsList() {
    return super.getHints();
  }

  public List<SchemaPath> getProjectedColumns() {
    return projectedColumns;
  }

  public double getObservedRowcountAdjustment() {
    return observedRowcountAdjustment;
  }

  /**
   * Allows specific implementations to reduce the cost of the read based on the properties of the
   * read.
   *
   * @return The amount to reduce the overall CPU cost of the operation.
   */
  public double getCostAdjustmentFactor() {
    return DEFAULT_COST_ADJUSTMENT;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return explainScanRel(
        pw, tableMetadata, projectedColumns, observedRowcountAdjustment, snapshotDiffContext);
  }

  /**
   * Given a TableMetadata, extract its Snapshot ID as a string
   *
   * @param tableMetadata input table metadata
   * @return SnapshotID as a string
   */
  public static String extractSnapshotID(TableMetadata tableMetadata) {
    return Optional.ofNullable(tableMetadata)
        .map(TableMetadata::getDatasetConfig)
        .map(DatasetConfig::getPhysicalDataset)
        .map(PhysicalDataset::getIcebergMetadata)
        .map(IcebergMetadata::getSnapshotId)
        .map(Object::toString)
        .orElse("");
  }

  public static RelWriter explainScanRel(
      RelWriter pw,
      TableMetadata tableMetadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      SnapshotDiffContext snapshotDiffContext) {
    pw.item("table", tableMetadata.getName());

    Optional.ofNullable(tableMetadata)
        .map(TableMetadata::getDatasetConfig)
        .map(DatasetConfig::getPhysicalDataset)
        .map(PhysicalDataset::getIcebergMetadata)
        .map(IcebergMetadata::getSnapshotId)
        .ifPresent(snapshotId -> pw.item("snapshot", snapshotId));

    Optional.ofNullable(tableMetadata)
        .map(TableMetadata::getVersionContext)
        .filter(x -> x != null)
        .ifPresent(versionContext -> pw.item("version", versionContext));

    Optional.ofNullable(snapshotDiffContext)
        .filter(SnapshotDiffContext::isEnabled)
        .ifPresent(
            x -> {
              final List<String> intervalsAsString = new ArrayList<>();
              for (final SnapshotDiffContext.SnapshotDiffSingleInterval interval :
                  x.getIntervals()) {
                final String beginSnapshotId =
                    extractSnapshotID(interval.getBeginningTableMetadata());
                final String endSnapshotId = extractSnapshotID(interval.getEndingTableMetadata());
                intervalsAsString.add("from " + beginSnapshotId + " to " + endSnapshotId);
              }
              final String filterApplyOptions =
                  Optional.ofNullable(x.getFilterApplyOptions()).map(Object::toString).orElse("");
              pw.item(
                  "snapshotDiffContext",
                  String.format(
                      "%s, %s", filterApplyOptions, String.join(", ", intervalsAsString)));
            });

    if (projectedColumns != null) {
      pw.item(
          "columns",
          FluentIterable.from(projectedColumns)
              .transform(
                  new Function<SchemaPath, String>() {

                    @Override
                    public String apply(SchemaPath input) {
                      return input.toString();
                    }
                  })
              .join(Joiner.on(", ")));
    }

    pw.item("splits", tableMetadata.getSplitCount());

    if (observedRowcountAdjustment != 1.0d) {
      pw.item("rowAdjust", observedRowcountAdjustment);
    }

    // we need to include the table metadata digest since not all properties (specifically which
    // splits) are included in the explain output  (what base computeDigest uses).
    pw.itemIf(
        "tableDigest",
        tableMetadata.computeDigest(),
        pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);

    return pw;
  }

  @Override
  public abstract RelNode copy(RelTraitSet traitSet, List<RelNode> inputs);

  public abstract ScanRelBase cloneWithProject(List<SchemaPath> projection);

  public boolean equalsTo(final Object other) {
    if (!(other instanceof ScanRelBase)) {
      return false;
    }

    if (!other.getClass().equals(this.getClass())) {
      return false;
    }

    ScanRelBase castOther = (ScanRelBase) other;

    return Objects.equal(projectedColumns, castOther.projectedColumns)
        && Objects.equal(getTable(), castOther.getTable())
        && Objects.equal(
            getTableMetadata().computeDigest(), castOther.getTableMetadata().computeDigest())
        && Objects.equal(getPluginId(), castOther.getPluginId());
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(this);
  }

  public BatchSchema getBatchSchema() {
    return getTableMetadata().getSchema();
  }

  public BatchSchema getProjectedSchema() {
    return getBatchSchema().maskAndReorder(projectedColumns);
  }

  public double getFilterReduction() {
    return 1.0d;
  }

  /**
   * Computes the cost honoring column pushdown and relative costs across tables.
   *
   * <p>The cost model is a function of row count, column and scan factors. Row count may or may not
   * be precise. Column factor is relative to number of columns to scan. Scan factor represents the
   * relative cost of making the scan as compared to other scans kinds.
   *
   * <p>An exact field contributes 1 whole points to column factor. A sub field scan contributes
   * proportionately anywhere from (0, 1], weighting subsequent sub fields less through a
   * logarithmic model.
   */
  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {

    final double rowCount =
        (PrelUtil.getPlannerSettings(this.getCluster()).useStatistics())
            ? mq.getRowCount(this)
            : estimateRowCount(mq);
    // If the estimatedCount is actually 0, then make it 1, so that at least, we choose the scan
    // that
    // has fewer columns pushed down since all the cost scales with rowCount.
    final double estimatedRowCount = Math.max(1, rowCount);

    final int fieldCount = getLeafColumnCount(tableMetadata.getSchema(), projectedColumns);

    double workCost =
        getCostAdjustmentFactor()
            * (rowCount
                * fieldCount
                * getTableMetadata().getReadDefinition().getScanStats().getScanFactor())
            * DremioCost.SCAN_CPU_COST_MULTIPLIER
            * getRowGroupFilterReduction();

    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return planner.getCostFactory().makeCost(estimatedRowCount, workCost, workCost);
    }

    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.
    // In the future we might consider alternative scans that go against projections or
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost.
    final DremioCost.Factory costFactory = (DremioCost.Factory) planner.getCostFactory();
    DremioCost cost = costFactory.makeCost(estimatedRowCount, workCost, workCost, workCost);
    Preconditions.checkArgument(!cost.isInfinite(), "infinite cost...");
    return cost;
  }

  /**
   * Just reduce this by a small fraction, so we pick this scan over any other scan which does not
   * have a row group filter
   *
   * @return the row group reduction factor.
   */
  private double getRowGroupFilterReduction() {
    if (this instanceof FilterableScan && ((FilterableScan) this).getRowGroupFilter() != null) {
      return PrelUtil.getPlannerSettings(getCluster()).getRowGroupFilterReductionFactor();
    }
    return 1.0d;
  }

  public static int getLeafColumnCount(BatchSchema schema) {
    // if this dataset is a text file using columns[] naming, set column factor to 1000 (since we
    // don't know any better).
    if (schema.isDeprecatedText()) {
      return MAX_COLUMNS;
    }

    return schema.getTotalFieldCount();
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  /**
   * Given an original schema and a list of projection columns determine the remaining field once
   * applying projection..
   *
   * @param schema
   * @param projectionColumns
   * @return
   */
  private static int getLeafColumnCount(BatchSchema schema, List<SchemaPath> projectionColumns) {
    if (projectionColumns != null) {
      return getLeafColumnCount(schema.maskAndReorder(projectionColumns));
    } else {
      return getLeafColumnCount(schema);
    }
  }

  private void setProjectedRowType(List<SchemaPath> projectedColumns) {
    if (projectedColumns != null) {
      this.rowType =
          getRowTypeFromProjectedColumns(projectedColumns, getBatchSchema(), getCluster());
    } else {
      this.rowType = deriveRowType();
    }
  }

  public StoragePluginId getIcebergStatisticsPluginId(OptimizerRulesContext context) {
    return null;
  }

  public static RelDataType getRowTypeFromProjectedColumns(
      List<SchemaPath> projectedColumns, BatchSchema schema, RelOptCluster cluster) {
    Set<String> firstLevelPaths = new LinkedHashSet<>();
    for (SchemaPath p : projectedColumns) {
      firstLevelPaths.add(p.getRootSegment().getNameSegment().getPath());
    }

    final RelDataTypeFactory factory = cluster.getTypeFactory();
    final FieldInfoBuilder builder = new FieldInfoBuilder(factory);
    final Map<String, RelDataType> fields = new HashMap<>();
    for (Field field : schema) {
      if (firstLevelPaths.contains(field.getName())) {
        fields.put(
            field.getName(),
            CalciteArrowHelper.wrap(CompleteType.fromField(field))
                .toCalciteType(
                    factory, PrelUtil.getPlannerSettings(cluster).isFullNestedSchemaSupport()));
      }
    }

    Preconditions.checkArgument(
        firstLevelPaths.size() == fields.size(),
        "Projected column base size %s is not equal to outcome rowtype %s.",
        firstLevelPaths.size(),
        fields.size());

    for (String path : firstLevelPaths) {
      builder.add(path, fields.get(path));
    }
    return builder.build();
  }

  public SnapshotDiffContext getSnapshotDiffContext() {
    return snapshotDiffContext;
  }
}
