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

import static com.dremio.exec.planner.common.ScanRelBase.getRowTypeFromProjectedColumns;
import static com.dremio.exec.store.SystemSchemas.CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA;
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Iceberg Snapshots reader prel. It generates snapshots based on different Snapshots reader modes.
 */
@Options
public class IcebergSnapshotsPrel extends AbstractRelNode implements LeafPrel {
  public static final TypeValidators.LongValidator RESERVE =
      new TypeValidators.PositiveLongValidator(
          "planner.op.scan.iceberg.snapshots.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT =
      new TypeValidators.PositiveLongValidator(
          "planner.op.scan.iceberg.snapshots.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private static final BatchSchema SNAPSHOTS_READER_SCHEMA =
      ICEBERG_SNAPSHOTS_SCAN_SCHEMA.merge(CARRY_FORWARD_FILE_PATH_TYPE_SCHEMA);
  private static final List<SchemaPath> PROJECTED_COLUMNS =
      SNAPSHOTS_READER_SCHEMA.getFields().stream()
          .map(f -> SchemaPath.getSimplePath(f.getName()))
          .collect(Collectors.toList());

  private final String user;
  private final RelDataType relDataType;
  private final SnapshotsScanOptions snapshotsScanOptions;
  private final long estimatedRows;
  private final int maxParallelizationWidth;
  private final StoragePluginId storagePluginId;
  private final Iterator<PartitionChunkMetadata> splits;

  public IcebergSnapshotsPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType relDataType,
      SnapshotsScanOptions snapshotsScanOptions,
      String user,
      StoragePluginId storagePluginId,
      Iterator<PartitionChunkMetadata> splits,
      long estimatedRows,
      int maxParallelizationWidth) {
    super(cluster, traitSet);
    this.relDataType = relDataType;
    this.snapshotsScanOptions =
        Preconditions.checkNotNull(snapshotsScanOptions, "snapshotsScanOption cannot be null");
    this.storagePluginId = storagePluginId;
    this.splits = splits;
    this.estimatedRows = estimatedRows;
    this.maxParallelizationWidth = maxParallelizationWidth;
    this.user = user;
  }

  public IcebergSnapshotsPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      SnapshotsScanOptions snapshotsScanOptions,
      String user,
      StoragePluginId storagePluginId,
      Iterator<PartitionChunkMetadata> splits,
      long estimatedRows,
      int maxParallelizationWidth) {
    this(
        cluster,
        traitSet,
        getRowTypeFromProjectedColumns(PROJECTED_COLUMNS, SNAPSHOTS_READER_SCHEMA, cluster),
        snapshotsScanOptions,
        user,
        storagePluginId,
        splits,
        estimatedRows,
        maxParallelizationWidth);
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    // The number of all snapshots in a table.
    return estimatedRows;
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new IcebergSnapshotsGroupScan(
        creator.props(this, user, SNAPSHOTS_READER_SCHEMA, RESERVE, LIMIT),
        storagePluginId,
        PROJECTED_COLUMNS,
        snapshotsScanOptions,
        splits,
        maxParallelizationWidth);
  }

  @Override
  public Prel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergSnapshotsPrel(
        getCluster(),
        getTraitSet(),
        snapshotsScanOptions,
        user,
        storagePluginId,
        splits,
        estimatedRows,
        maxParallelizationWidth);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value)
      throws E {
    return logicalVisitor.visitLeaf(this, value);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return maxParallelizationWidth;
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.NONE;
  }

  @Override
  protected RelDataType deriveRowType() {
    return relDataType;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    if (snapshotsScanOptions != null) {
      pw.item("options value", snapshotsScanOptions);
    }

    return pw;
  }
}
