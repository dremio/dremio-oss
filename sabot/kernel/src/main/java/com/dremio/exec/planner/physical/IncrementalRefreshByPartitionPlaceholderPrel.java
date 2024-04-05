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
package com.dremio.exec.planner.physical;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * This class represents a placeholder for a plan that will be generated in the future It will be
 * replaced by the actual plan once it is generated
 */
public class IncrementalRefreshByPartitionPlaceholderPrel extends AbstractRelNode implements Prel {

  private final SnapshotDiffContext snapshotDiffContext;

  public IncrementalRefreshByPartitionPlaceholderPrel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelDataType rowType,
      final SnapshotDiffContext snapshotDiffContext) {
    super(cluster, traitSet);
    this.rowType = rowType;
    this.snapshotDiffContext = snapshotDiffContext;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
    return new IncrementalRefreshByPartitionPlaceholderPrel(
        getCluster(), traitSet, rowType, snapshotDiffContext);
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E {
    return logicalVisitor.visitPrel(this, value);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator)
      throws IOException {
    throw UserException.unsupportedError()
        .message(
            "An IncrementalRefreshByPartitionPlaceholderPrel does not have a physical operator."
                + " It should have been replaced by a corresponding plan earlier.")
        .buildSilently();
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  public SnapshotDiffContext getSnapshotDiffContext() {
    return snapshotDiffContext;
  }
}
