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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class WriterCommitterPrel extends SingleRel implements Prel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.writercommiter.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.writercommiter.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private final String tempLocation;
  private final String finalLocation;
  private final FileSystemPlugin<?> plugin;
  private final String userName;
  private FileSystemCreateTableEntry fileSystemCreateTableEntry;

  public WriterCommitterPrel(RelOptCluster cluster,
                             RelTraitSet traits,
                             RelNode child,
                             FileSystemPlugin<?> plugin,
                             String tempLocation,
                             String finalLocation,
                             String userName,
                             FileSystemCreateTableEntry fileSystemCreateTableEntry) {
    super(cluster, traits, child);
    this.tempLocation = tempLocation;
    this.finalLocation = finalLocation;
    this.plugin = plugin;
    this.userName = userName;
    this.fileSystemCreateTableEntry = fileSystemCreateTableEntry;
  }

  @Override
  public WriterCommitterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WriterCommitterPrel(getCluster(), traitSet, sole(inputs), plugin, tempLocation, finalLocation, userName, fileSystemCreateTableEntry);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .itemIf("temp", tempLocation, tempLocation != null)
      .item("final", finalLocation);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator childPop = child.getPhysicalOperator(creator);
    return new WriterCommitterPOP(
      creator.props(this, userName, RecordWriter.SCHEMA, RESERVE, LIMIT),
      tempLocation,
      finalLocation,
      fileSystemCreateTableEntry.getIcebergTableProps(),
      childPop,
      (FileSystemPlugin<?>) plugin
    );
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitWriterCommitter(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  public String getUserName() {
    return userName;
  }
}
