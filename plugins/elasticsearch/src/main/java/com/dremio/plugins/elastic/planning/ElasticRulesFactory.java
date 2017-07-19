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
package com.dremio.plugins.elastic.planning;

import java.util.Set;

import org.apache.calcite.plan.RelOptRule;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.server.options.OptionManager;
import com.dremio.exec.store.StoragePluginTypeRulesFactory;
import com.dremio.plugins.elastic.planning.rules.ElasticFilterRule;
import com.dremio.plugins.elastic.planning.rules.ElasticLimitRule;
import com.dremio.plugins.elastic.planning.rules.ElasticProjectRule;
import com.dremio.plugins.elastic.planning.rules.ElasticSampleRule;
import com.dremio.plugins.elastic.planning.rules.ElasticScanPrule;
import com.dremio.plugins.elastic.planning.rules.ElasticScanRule;
import com.dremio.service.namespace.StoragePluginType;
import com.google.common.collect.ImmutableSet;

public class ElasticRulesFactory implements StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, StoragePluginType pluginType) {

    final OptionManager options = optimizerContext.getPlannerSettings().getOptions();

    switch(phase){
    case LOGICAL:
      return ImmutableSet.<RelOptRule>of(new ElasticScanRule(pluginType));

    case PHYSICAL:

      ImmutableSet.Builder<RelOptRule> builder = ImmutableSet.builder();
      builder.add(new ElasticScanPrule(optimizerContext.getFunctionRegistry()));

      if (options.getOption(ExecConstants.ELASTIC_RULES_PROJECT)) {
        builder.add(new ElasticProjectRule(optimizerContext.getFunctionRegistry()));
      }
      if (options.getOption(ExecConstants.ELASTIC_RULES_FILTER)) {
        builder.add(ElasticFilterRule.INSTANCE);
      }

      if (options.getOption(ExecConstants.ELASTIC_RULES_LIMIT)) {
        builder.add(ElasticLimitRule.INSTANCE);
      }

      if (options.getOption(ExecConstants.ELASTIC_RULES_SAMPLE)) {
        builder.add(ElasticSampleRule.INSTANCE);
      }

      return builder.build();

    default:
      return ImmutableSet.of();
    }
  }

}
