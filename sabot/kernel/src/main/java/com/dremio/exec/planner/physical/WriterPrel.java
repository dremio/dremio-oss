/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.WriterRelBase;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;

public class WriterPrel extends WriterRelBase implements Prel {

  public static final String PARTITION_COMPARATOR_FIELD = "P_A_R_T_I_T_I_O_N_C_O_M_P_A_R_A_T_O_R";
  public static final String BUCKET_NUMBER_FIELD = "P_A_R_T_I_T_I_O_N_N_U_M_B_E_R";
  public static final String PARTITION_COMPARATOR_FUNC = "newPartitionValue";

  private final RelDataType expectedInboundRowType;

  public WriterPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, CreateTableEntry createTableEntry, RelDataType expectedInboundRowType) {
    super(Prel.PHYSICAL, cluster, traits, child, createTableEntry);
    this.expectedInboundRowType = expectedInboundRowType;
  }

  @Override
  public WriterPrel copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WriterPrel(getCluster(), traitSet, sole(inputs), getCreateTableEntry(), expectedInboundRowType);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();
    PhysicalOperator g = getCreateTableEntry().getWriter(child.getPhysicalOperator(creator));
    return creator.addMetadata(this, g);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitWriter(this, value);
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

  public RelDataType getExpectedInboundRowType() {
    return expectedInboundRowType;
  }

}
