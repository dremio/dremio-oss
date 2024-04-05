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

import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class ProjectPrule extends Prule {
  public static final RelOptRule INSTANCE = new ProjectPrule();

  private ProjectPrule() {
    super(RelOptHelper.some(ProjectRel.class, RelOptHelper.any(RelNode.class)), "ProjectPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = (ProjectRel) call.rel(0);
    final RelNode input = project.getInput();

    RelTraitSet traits = input.getTraitSet().plus(Prel.PHYSICAL);
    RelNode convertedInput = convert(input, traits);

    boolean traitPull = new ProjectTraitPull(call).go(project, convertedInput);

    // If ProjectTraitPull didn't transform, let's do a conversion anyway
    if (!traitPull) {
      call.transformTo(
          ProjectPrel.create(
              project.getCluster(),
              convertedInput.getTraitSet(),
              convertedInput,
              project.getProjects(),
              project.getRowType()));
    }
  }

  private class ProjectTraitPull extends SubsetTransformer<ProjectRel, RuntimeException> {

    public ProjectTraitPull(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(ProjectRel project, RelNode rel) throws RuntimeException {
      RelTraitSet newProjectTraits = newTraitSet(Prel.PHYSICAL);
      return ProjectPrel.create(
          project.getCluster(), newProjectTraits, rel, project.getProjects(), project.getRowType());
    }
  }
}
