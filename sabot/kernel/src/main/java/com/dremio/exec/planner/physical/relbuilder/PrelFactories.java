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
package com.dremio.exec.planner.physical.relbuilder;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.google.common.base.Preconditions;

public interface PrelFactories {
  RelFactories.FilterFactory FILTER = (child, condition, correlVariables) -> {
    Preconditions.checkArgument(correlVariables.isEmpty());
    return FilterPrel.create(child.getCluster(), child.getTraitSet(), child, condition);
  };

  RelFactories.ProjectFactory PROJECT = (input, hints, childExprs, fieldNames) -> {
    //TODO fix this
    final RelTraitSet traits = input.getTraitSet().replace(RelCollations.EMPTY);
    final List<RexNode> noExtend = new ArrayList<>(childExprs);
    final RelDataType type =
        RexUtil.createStructType(input.getCluster().getTypeFactory(), noExtend);
    return ProjectPrel.create(input.getCluster(), traits, input, noExtend, type);
  };

}
