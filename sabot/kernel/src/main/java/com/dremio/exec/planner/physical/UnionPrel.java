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

import com.dremio.exec.planner.common.UnionRelBase;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;

public abstract class UnionPrel extends UnionRelBase implements Prel {

  public UnionPrel(
      RelOptCluster cluster,
      RelTraitSet traits,
      List<RelNode> inputs,
      boolean all,
      boolean checkCompatibility)
      throws InvalidRelException {
    super(cluster, traits, inputs, all, checkCompatibility);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value)
      throws E {
    return logicalVisitor.visitUnion(this, value);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(this.getInputs());
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return false;
  }
}
