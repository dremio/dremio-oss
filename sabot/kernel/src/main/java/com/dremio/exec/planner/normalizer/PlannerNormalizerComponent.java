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
package com.dremio.exec.planner.normalizer;

import org.apache.calcite.rel.RelNode;

/**
 * Component for Normalizing {@link RelNode} to a common logical form.
 */
public class PlannerNormalizerComponent {

  private final RelNormalizerTransformer relNormalizerTransformer;

  public PlannerNormalizerComponent(RelNormalizerTransformer relNormalizerTransformer) {
    this.relNormalizerTransformer = relNormalizerTransformer;
  }

  public RelNormalizerTransformer getRelNormalizerTransformer() {
    return relNormalizerTransformer;
  }

  public static PlannerNormalizerComponent build(
    PlannerBaseComponent plannerBaseComponent,
    PlannerNormalizerModule plannerNormalizerModule
  ) {

    NormalizerRuleSets normalizerRuleSet = plannerNormalizerModule.buildNormalizeRuleSets(
      plannerBaseComponent.getPlannerSettings(),
      plannerBaseComponent.getUserDefinedFunctionExpander(),
      plannerBaseComponent.getFunctionContext().getContextInformation());

    RelNormalizerTransformer relNormalizerTransformer = plannerNormalizerModule.buildRelNormalizerTransformer(
      plannerBaseComponent.getHepPlannerRunner(),
      normalizerRuleSet,
      plannerBaseComponent.getPlannerSettings());

    return new PlannerNormalizerComponent(relNormalizerTransformer);
  }
}
