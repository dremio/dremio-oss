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
package com.dremio.exec.planner.logical;

import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;

/**
 * This version of ProjectFilterTranspose rule only pushes down column references
 */
public class ProjectInputRefPastFilterRule extends ProjectFilterTransposeRule {
  public ProjectInputRefPastFilterRule() {
    super(Config.DEFAULT.withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
      .as(Config.class)
      .withOperandFor(ProjectRel.class, FilterRel.class, JoinRel.class)
      .withPreserveExprCondition(Conditions.PUSH_REX_INPUT_REF));

  }
}
