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

import java.io.IOException;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import com.dremio.exec.planner.common.FilterRelBase;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticIntermediateScanPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchFilter;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchProject;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;
import com.dremio.plugins.elastic.planning.rules.PredicateAnalyzer.Residue;

public class ElasticFilterRule extends RelOptRule {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticFilterRule.class);

  public static final ElasticFilterRule INSTANCE = new ElasticFilterRule();

  private ElasticFilterRule() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ElasticsearchIntermediatePrel.class)), "ElasticFilterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final FilterPrel filter = call.rel(0);
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);

    if (intermediatePrel.hasTerminalPrel() || intermediatePrel.contains(ElasticsearchFilter.class)) {
      return false;
    }

    // more checking will be done in onmatch.
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    try {
      Residue residue = tryWithResidue(call, Residue.NONE);
      if (!residue.none()) {
        tryWithResidue(call, residue);
      }
    } catch (ExpressionNotAnalyzableException | IOException e) {
      // Nothing could be transformed or we encountered a fatal error. The query will
      // still execute, but it will be slow b/c we can't push the predicate down to elasticsearch.
      logger.debug("Failed to push filter into scan/project; falling back to manual filtering", e);
      return;
    }
  }

  private Residue tryWithResidue(RelOptRuleCall call, Residue residue) throws ExpressionNotAnalyzableException, IOException {
    final FilterRelBase filter = call.rel(0);

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexNode originalCondition = filter.getCondition();

    final RexNode condition = residue.getNewPredicate(originalCondition, rexBuilder);

    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
    final ElasticRuleRelVisitor visitor = new FilterConverterVisitor(rexBuilder, condition, intermediatePrel.getInput()).go();
    final ElasticsearchPrel newInput = visitor.getConvertedTree();
    final ElasticsearchIntermediatePrel newInter = intermediatePrel.withNewInput(newInput);

    final ElasticsearchFilter newFilter = newInter.get(ElasticsearchFilter.class);
    final ElasticIntermediateScanPrel scan = newInter.get(ElasticIntermediateScanPrel.class);

    Residue newResidue = PredicateAnalyzer.analyzeConjunctions(scan, newFilter.getCondition());
    if (newResidue.none()) {
      PredicateAnalyzer.analyze(scan, newFilter.getCondition(), false);

      final RexNode residueCondition = residue.getResidue(originalCondition, rexBuilder);
      if (!residueCondition.isAlwaysTrue()) {
        final Filter residueFilter = filter.copy(filter.getTraitSet(), newInter, residueCondition);
        call.transformTo(residueFilter);
      } else {
        call.transformTo(newInter);
      }
      return Residue.NONE;
    } else {
      RexNode newPredicate = newResidue.getNewPredicate(originalCondition, rexBuilder);
      if (newPredicate.isAlwaysTrue()) {
        throw new ExpressionNotAnalyzableException(String.format("Could not push down any part of condition: %s", filter.getCondition()), null);
      }
      return newResidue;
    }
  }

  /**
   * Visits the elasticsearch subtree and collapses the tree.
   *
   * FilterConverterVisitor is called when a new ElasticsearchFilter has been pushed down, and it will collapse
   * all the filters in the subtree into a single filter, and push the filter past the project to sit right on top
   * of the scan.
   */
  class FilterConverterVisitor extends ElasticRuleRelVisitor {

    protected final RexBuilder rexBuilder;

    FilterConverterVisitor(RexBuilder rexBuilder, RexNode condition, RelNode input) {
      super(input);
      this.rexBuilder = rexBuilder;
      this.filterExprs = condition;
    }

    @Override
    public void processFilter(ElasticsearchFilter filter) {
      filterExprs = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterExprs, filter.getCondition());
    }

    @Override
    public void processProject(ElasticsearchProject project) {
      filterExprs = pushPastProject(filterExprs, project);
      if (projectExprs == null) {
        projectExprs = project.getProjects();
        projectDataType = project.getRowType();
      } else {
        projectExprs = RelOptUtil.pushPastProject(projectExprs, project);
        // projectDataType should not be set here, since we want to keep the top project's row type.
      }
    }

    @Override
    public void processSample(ElasticsearchSample node) {
      parents.add((ElasticsearchPrel) node);
    }
  }

  // copied these methods from Calcite's RelOptUtil.putPastProject, because it is important that ordering of the conjunctions is unchanged.
  // we want to protect ourselves from future changes to Calcite
  public static RexNode pushPastProject(RexNode node, Project project) {
    return node.accept(pushShuttle(project));
  }

  private static RexShuttle pushShuttle(final Project project) {
    return new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef ref) {
        return project.getProjects().get(ref.getIndex());
      }
    };
  }

}
