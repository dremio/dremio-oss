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
package com.dremio.exec.planner.rules;

import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.ProjectRel;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;

public class AggregateProjectMergeDrule extends AggregateProjectMergeRule {
  public AggregateProjectMergeDrule() {
    super(AggregateRel.class, ProjectRel.class, DremioRelFactories.LOGICAL_BUILDER);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    AggregateRel agg = call.rel(0);
    return Aggregate.isSimple(agg);
  }
}
