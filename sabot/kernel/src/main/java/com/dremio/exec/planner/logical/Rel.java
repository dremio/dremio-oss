/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.planner.physical.Prel;

/**
 * Relational expression that is implemented in Dremio.
 */
public interface Rel extends RelNode {
  /** Calling convention for relational expressions that are "implemented" by
   * generating Dremio logical plans. */
  public static final Convention LOGICAL = new Convention.Impl("LOGICAL", Rel.class) {
    @Override
    public boolean canConvertConvention(Convention toConvention) {
      return (toConvention == Prel.PHYSICAL || toConvention == LOGICAL);
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits) {
      return canConvertConvention((Convention) toTraits.getTrait(this.getTraitDef()));
    }
  };

  LogicalOperator implement(LogicalPlanImplementor implementor);
}
