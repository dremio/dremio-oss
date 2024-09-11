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

import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;

public class PlannerNormalizerModule {

  public RelNormalizerTransformer buildRelNormalizerTransformer(
      HepPlannerRunner hepPlannerRunner,
      NormalizerRuleSets normalizerRuleSets,
      SubstitutionProvider substitutionProvider,
      ViewExpansionContext viewExpansionContext,
      PlannerSettings plannerSettings,
      PlannerCatalog plannerCatalog,
      PlannerEventBus plannerEventBus) {
    return new RelNormalizerTransformerImpl(
        hepPlannerRunner,
        normalizerRuleSets,
        substitutionProvider,
        viewExpansionContext,
        plannerSettings,
        plannerCatalog,
        plannerEventBus);
  }

  public NormalizerRuleSets buildNormalizeRuleSets(
      OptionResolver optionResolver,
      UserDefinedFunctionExpander userDefinedFunctionExpander,
      ContextInformation contextInformation) {
    return new NormalizerRuleSets(optionResolver, userDefinedFunctionExpander, contextInformation);
  }
}
