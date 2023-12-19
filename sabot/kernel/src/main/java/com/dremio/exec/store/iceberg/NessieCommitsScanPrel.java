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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.VacuumOutputSchema;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.google.common.base.Preconditions;

/**
 * Scan on Nessie Commits, given a criteria.
 */
@Options
public class NessieCommitsScanPrel extends AbstractRelNode implements LeafPrel {
  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.nessie.commits.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.nessie.commits.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private final String user;
  private final RelDataType relDataType;
  private final SnapshotsScanOptions snapshotsScanOptions;
  private final long estimatedRows;
  private final int maxParallelizationWidth;
  private final StoragePluginId storagePluginId;
  private final BatchSchema batchSchema;

  public NessieCommitsScanPrel(
    RelOptCluster cluster,
    RelTraitSet traitSet,
    BatchSchema batchSchema,
    SnapshotsScanOptions snapshotsScanOptions,
    String user,
    StoragePluginId storagePluginId,
    long estimatedRows,
    int maxParallelizationWidth) {
    super(cluster, traitSet);
    this.batchSchema = batchSchema;
    this.relDataType = VacuumOutputSchema.getRowType(batchSchema, cluster.getTypeFactory());
    this.snapshotsScanOptions = Preconditions.checkNotNull(snapshotsScanOptions);
    this.storagePluginId = storagePluginId;
    this.estimatedRows = estimatedRows;
    this.maxParallelizationWidth = maxParallelizationWidth;
    this.user = user;
  }

  public RelDataType getRelDataType() {
    return relDataType;
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
    return new NessieCommitsGroupScan(
      creator.props(this, user, batchSchema, RESERVE, LIMIT),
      storagePluginId,
      SchemaUtilities.allColPaths(batchSchema),
      snapshotsScanOptions,
      maxParallelizationWidth);
  }

  @Override
  public Prel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new NessieCommitsScanPrel(
      getCluster(),
      getTraitSet(),
      batchSchema,
      snapshotsScanOptions,
      user,
      storagePluginId,
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
    if (snapshotsScanOptions != null) {
      pw.item("options", snapshotsScanOptions);
    }

    return pw;
  }

  @Override
  public int getEstimatedSize() {
    return (int) estimatedRows;
  }
}
