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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;

public interface Prel extends RelNode, Iterable<Prel>{

  final long DEFAULT_RESERVE = 1_000_000;
  final long DEFAULT_LIMIT = Long.MAX_VALUE;
  final long DEFAULT_LOW_LIMIT = 0;

  Convention PHYSICAL = new Convention.Impl("PHYSICAL", Prel.class) {
    @Override
    public boolean canConvertConvention(Convention toConvention) {
      // TODO should think about how we can get rid of "toConvention == LOGICAL" here.  It shouldn't be needed, but
      // currently, our Dremio ProjectPrule matches on ProjectRel + Any, and without the abstract converters added,
      // this rule may not be queued to create the necessary ProjectPrels later.
      return (toConvention == PHYSICAL || toConvention == Rel.LOGICAL);
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
      return canConvertConvention((Convention) toTraits.getTrait(this.getTraitDef()));
    }
  };

  PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException;

  <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E;

  /**
   * Supported 'encodings' of a Prel indicates what are the acceptable modes of SelectionVector
   * of its child Prel
   */
  SelectionVectorMode[] getSupportedEncodings();

  /**
   * A Prel's own SelectionVector mode - i.e whether it generates an SV2, SV4 or None
   */
  SelectionVectorMode getEncoding();

  boolean needsFinalColumnReordering();

  default double getCostForParallelization() {
    return getCluster().getMetadataQuery().getRowCount(this);
  }

  //
  // DRILL-3011
  // Prel copy(RelTraitSet paramRelTraitSet, List<RelNode> paramList);
}
