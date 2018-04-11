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
package com.dremio.plugins.elastic.planning.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.SamplePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchSample;

public class ElasticSampleRule extends RelOptRule {

  public static final ElasticSampleRule INSTANCE = new ElasticSampleRule();

  public static final long SAMPLE_SIZE_DENOMINATOR = 10;

  public ElasticSampleRule() {
    super(RelOptHelper.some(SamplePrel.class, RelOptHelper.any(ElasticsearchIntermediatePrel.class)), "ElasticSampleRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
    assert !intermediatePrel.contains(ElasticsearchSample.class) : "Cannot have more than one sample per scan";

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ElasticsearchIntermediatePrel intermediatePrel = call.rel(1);
    final ElasticsearchSample newSample = new ElasticsearchSample(
        intermediatePrel.getInput().getCluster(),
        intermediatePrel.getInput().getTraitSet(),
        intermediatePrel.getInput(),
        intermediatePrel.getPluginId());
    final ElasticsearchIntermediatePrel newInter = intermediatePrel.withNewInput(newSample);
    call.transformTo(newInter);
  }
}
