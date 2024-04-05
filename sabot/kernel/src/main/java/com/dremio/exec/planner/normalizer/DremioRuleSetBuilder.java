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

import com.dremio.options.OptionResolver;
import com.dremio.options.TypeValidators;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class DremioRuleSetBuilder {
  private final ImmutableList.Builder<RelOptRule> rules = ImmutableList.builder();

  private final OptionResolver optionResolver;

  public DremioRuleSetBuilder(OptionResolver optionResolver) {
    this.optionResolver = optionResolver;
  }

  public DremioRuleSetBuilder add(RelOptRule rule) {
    rules.add(rule);
    return this;
  }

  public DremioRuleSetBuilder addAll(List<RelOptRule> rules) {
    for (RelOptRule rule : rules) {
      this.add(rule);
    }

    return this;
  }

  public DremioRuleSetBuilder add(RelOptRule rule, boolean needed) {
    if (needed) {
      rules.add(rule);
    }
    return this;
  }

  public DremioRuleSetBuilder add(
      RelOptRule rule, TypeValidators.BooleanValidator booleanValidator) {
    if (optionResolver.getOption(booleanValidator)) {
      rules.add(rule);
    }
    return this;
  }

  public RuleSet build() {
    return RuleSets.ofList(rules.build());
  }
}
