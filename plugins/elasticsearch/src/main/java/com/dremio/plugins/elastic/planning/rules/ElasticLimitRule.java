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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;

import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchLimit;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;
import com.google.common.base.Predicate;

public class ElasticLimitRule extends RelOptRule {

  public static final ElasticLimitRule INSTANCE = new ElasticLimitRule();

  public ElasticLimitRule() {
    super(RelOptHelper.some(LimitPrel.class, RelOptHelper.any(ElasticsearchIntermediatePrel.class)), "ElasticLimitRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final LimitPrel limit = call.rel(0);
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);

    if (intermediatePrel.hasTerminalPrel()) {
      return false;
    }

    // TODO: this can probably be supported in many cases.
    if (limit.getOffset() != null && RexLiteral.intValue(limit.getOffset()) != 0) {
      return false;
    }

    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(limit.getCluster().getPlanner());
    if (intermediatePrel.contains(ElasticsearchSample.class)
        && limit.getFetch() != null
        && RexLiteral.intValue(limit.getFetch()) >= SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, ElasticSampleRule.SAMPLE_SIZE_DENOMINATOR)) {
      return false;
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LimitPrel limitPrel = call.rel(0);
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);

    final ElasticsearchLimit newLimit = new ElasticsearchLimit(
        intermediatePrel.getInput().getCluster(),
        intermediatePrel.getInput().getTraitSet(),
        intermediatePrel.getInput(),
        limitPrel.getOffset(),
        limitPrel.getFetch(),
        limitPrel.isPushDown(),
        intermediatePrel.getPluginId());

    final ElasticsearchSample sample = intermediatePrel.getNoCheck(ElasticsearchSample.class);
    if(sample != null){
      // we do not allow a sample and limit to coexist in the elastic tree, need to collapse them.
      final ElasticsearchIntermediatePrel withoutSample = intermediatePrel.filter(new Predicate<RelNode>(){
        @Override
        public boolean apply(RelNode input) {
          return !(input instanceof ElasticsearchSample);
        }});
      final ElasticsearchLimit mergedLimitSample = newLimit.merge(sample, withoutSample.getInput());
      call.transformTo(intermediatePrel.withNewInput(mergedLimitSample));
    } else {
      final ElasticsearchIntermediatePrel newInter = intermediatePrel.withNewInput(newLimit);
      call.transformTo(newInter);
    }
  }
}
