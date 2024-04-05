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
package com.dremio.plugins.elastic.planning.rules;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchFilter;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchProject;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;

/**
 * Elastic project pushdown rule
 *
 * <p>This rule starts by finding grabbing any existing projects from the Elastic subtree. It then
 * merges the found project with the existing project (if found). From there, it does a rewrite of
 * the expression tree to determine if the particular expression can be pushed down.
 */
public class ElasticProjectRule extends RelOptRule {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ElasticProjectRule.class);

  private final FunctionLookupContext functionLookupContext;

  public ElasticProjectRule(FunctionLookupContext functionLookupContext) {
    super(
        RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ElasticsearchIntermediatePrel.class)),
        "ElasticProjectRule");
    this.functionLookupContext = functionLookupContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);

    if (intermediatePrel.hasTerminalPrel()) {
      return false;
    }

    // more checks in onMatch but multiple work can be done with same operations. (Basically, we
    // need to try to pushdown to know if we can, no reason to do twice.)
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectPrel project = call.rel(0);
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
    final ElasticRuleRelVisitor visitor =
        new ProjectConverterVisitor(project, intermediatePrel.getInput()).go();
    final ElasticsearchPrel newInput = visitor.getConvertedTree();
    final ElasticsearchIntermediatePrel newInter =
        new ElasticsearchIntermediatePrel(newInput.getTraitSet(), newInput, functionLookupContext);

    // we pushdown no matter what. This is because we will later pop up.
    call.transformTo(newInter);
  }

  /**
   * Visits the elasticsearch subtree and collapses the tree.
   *
   * <p>ProjectConverterVisitor is called when a new ElasticsearchProject has been pushed down, and
   * it will collapse all the projects in the subtree into a single project.
   */
  class ProjectConverterVisitor extends ElasticRuleRelVisitor {
    ProjectConverterVisitor(Project project, RelNode input) {
      super(input);
      projectExprs = project.getProjects();
      projectDataType = project.getRowType();
    }

    @Override
    public void processFilter(ElasticsearchFilter filter) {
      child = filter;
      continueToChildren = false;
    }

    @Override
    public void processProject(ElasticsearchProject project) {
      projectExprs = RelOptUtil.pushPastProject(projectExprs, project);
      // projectDataType should not be set here, since we want to keep the top project's row type.
    }

    @Override
    public void processSample(ElasticsearchSample node) {
      parents.add(node);
    }
  }
}
