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
import static com.dremio.exec.store.SystemSchemas.ICEBERG_SNAPSHOTS_SCAN_SCHEMA;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.google.common.base.Preconditions;

/**
 * Iceberg Snapshots reader prel. It generates snapshots based on different Snapshots reader modes.
 */
@Options
public class IcebergSnapshotsPrel extends AbstractRelNode implements LeafPrel {
  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.iceberg.snapshots.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.iceberg.snapshots.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private static final BatchSchema SNAPSHOTS_READER_SCHEMA = ICEBERG_SNAPSHOTS_SCAN_SCHEMA;
  private static final List<SchemaPath> PROJECTED_COLUMNS = SNAPSHOTS_READER_SCHEMA.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList());

  protected final TableMetadata tableMetadata;
  private final BatchSchema schema;
  private final List<SchemaPath> projectedColumns;
  private final RelDataType relDataType;
  private final IcebergTableProps icebergTableProps;
  private final SnapshotsScanOptions snapshotsScanOptions;
  private final long estimatedRows;
  private final int maxParallelizationWidth;


  public IcebergSnapshotsPrel(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    TableMetadata tableMetadata,
    BatchSchema schema,
    List<SchemaPath> projectedColumns,
    RelDataType relDataType,
    IcebergTableProps icebergTableProps,
    SnapshotsScanOptions snapshotsScanOptions,
    long estimatedRows,
    int maxParallelizationWidth) {
    super(cluster, traitSet);
    this.tableMetadata = tableMetadata;
    this.schema = schema;
    this.projectedColumns = projectedColumns;
    this.relDataType = relDataType;
    this.icebergTableProps = icebergTableProps;
    this.snapshotsScanOptions = Preconditions.checkNotNull(snapshotsScanOptions, "snapshotsScanOption cannot be null");
    this.estimatedRows = estimatedRows;
    this.maxParallelizationWidth = maxParallelizationWidth;
  }

  public IcebergSnapshotsPrel(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    TableMetadata tableMetadata,
    IcebergTableProps icebergTableProps,
    SnapshotsScanOptions snapshotsScanOptions,
    long estimatedRows,
    int maxParallelizationWidth) {
    this(cluster, traitSet, tableMetadata, SNAPSHOTS_READER_SCHEMA, PROJECTED_COLUMNS,
      getRowTypeFromProjectedColumns(PROJECTED_COLUMNS, SNAPSHOTS_READER_SCHEMA, cluster), icebergTableProps,
      snapshotsScanOptions, estimatedRows, maxParallelizationWidth);
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
      creator.props(this, tableMetadata.getUser(), schema, RESERVE, LIMIT),
      tableMetadata,
      icebergTableProps,
      projectedColumns,
      snapshotsScanOptions);
  }

  @Override
  public Prel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new IcebergSnapshotsPrel(
      getCluster(),
      getTraitSet(),
      tableMetadata,
      schema,
      projectedColumns,
      relDataType,
      icebergTableProps,
      snapshotsScanOptions,
      estimatedRows,
      maxParallelizationWidth);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
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
    pw = ScanRelBase.explainScanRel(pw, tableMetadata, null, 1.0);

    /* To avoid NPE in the method chain Optional is used*/
    Optional<String> metadataLocation = Optional.ofNullable(tableMetadata.getDatasetConfig())
      .map(DatasetConfig::getPhysicalDataset)
      .map(PhysicalDataset::getIcebergMetadata)
      .map(IcebergMetadata::getMetadataFileLocation);

    if (metadataLocation.isPresent()) {
      pw.item("metadataFileLocation", metadataLocation.get());
    }
    if (snapshotsScanOptions != null) {
      pw.item("options value", snapshotsScanOptions);
    }

    return pw;
  }
}
