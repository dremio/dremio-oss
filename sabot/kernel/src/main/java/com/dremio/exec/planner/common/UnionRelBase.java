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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;

/**
 * Base class for logical and physical Union implemented in Dremio
 */
public abstract class UnionRelBase extends Union {

  public UnionRelBase(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, boolean all, boolean checkCompatibility) throws InvalidRelException {
    super(cluster, traits, inputs, all);
    if (checkCompatibility &&
        !this.isCompatible(cluster, false /* don't compare names */, true /* allow substrings */)) {
      throw new InvalidRelException("Input row types of the Union are not compatible.");
    }
  }

  public boolean isCompatible(RelOptCluster cluster, boolean compareNames, boolean allowSubstring) {
    RelDataType unionType = getRowType();
    PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster);
    for (RelNode input : getInputs()) {
      if (! MoreRelOptUtil.areRowTypesCompatible(
        input.getRowType(), unionType, compareNames, allowSubstring)) {
        return false;
      }
    }
    return true;
  }
}
