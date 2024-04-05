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
package com.dremio.exec.planner.physical.rule;

import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.NestedLoopJoinPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.relbuilder.PrelBuilderFactory;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;

/**
 * Capture Filter - Project - NLJ pattern and swap Filter and Project
 *
 * @link {org.apache.calcite.rel.rules.FilterProjectTransposeRule}
 */
public class FilterProjectTransposePRule {
  public static final RelOptRule FILTER_PROJECT_NLJ =
      FilterProjectTransposeRule.Config.DEFAULT
          .withRelBuilderFactory(PrelBuilderFactory.INSTANCE)
          .withOperandSupplier(
              os1 ->
                  os1.operand(FilterPrel.class)
                      .oneInput(
                          os2 ->
                              os2.operand(ProjectPrel.class)
                                  .oneInput(
                                      os3 -> os3.operand(NestedLoopJoinPrel.class).anyInputs())))
          .as(FilterProjectTransposeRule.Config.class)
          .toRule();
}
