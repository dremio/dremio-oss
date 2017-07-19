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
package com.dremio.plugins.elastic.planning.rules;

import java.io.IOException;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.elasticsearch.index.query.QueryBuilder;

import com.dremio.exec.planner.common.FilterRelBase;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticIntermediateScanPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchFilter;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchProject;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;

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

    if (intermediatePrel.hasTerminalPrel()) {
      return false;
    }

    // more checking will be done in onmatch.
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRelBase filter = call.rel(0);
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
    final ElasticRuleRelVisitor visitor = new FilterConverterVisitor(filter, intermediatePrel.getInput()).go();
    final ElasticsearchPrel newInput = visitor.getConvertedTree();
    final ElasticsearchIntermediatePrel newInter = intermediatePrel.withNewInput(newInput);

    final ElasticsearchFilter newFilter = newInter.get(ElasticsearchFilter.class);
    final ElasticIntermediateScanPrel scan = newInter.get(ElasticIntermediateScanPrel.class);

    try {
      QueryBuilder analyzed = PredicateAnalyzer.analyze(scan, newFilter.getCondition());
      PredicateAnalyzer.queryAsJson(analyzed);
      call.transformTo(newInter);
      return;
    } catch (ExpressionNotAnalyzableException | IOException e) {
      // Nothing could be transformed or we encountered a fatal error. The query will
      // still execute, but it will be slow b/c we can't push the predicate down to elasticsearch.
      logger.debug("Failed to push filter into scan/project; falling back to manual filtering", e);
      return;
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

    FilterConverterVisitor(Filter filter, RelNode input) {
      super(input);
      this.rexBuilder = filter.getCluster().getRexBuilder();
      this.filterExprs = filter.getCondition();
    }

    @Override
    public void processFilter(ElasticsearchFilter filter) {
      filterExprs = rexBuilder.makeCall(SqlStdOperatorTable.AND, filterExprs, filter.getCondition());
    }

    @Override
    public void processProject(ElasticsearchProject project) {
      filterExprs = RelOptUtil.pushPastProject(filterExprs, project);
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

}
