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
package com.dremio.exec.planner.common;

import java.util.List;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.google.common.collect.Lists;

public class ReduceExpressionsUtil extends ReduceExpressionsRule {

  private ReduceExpressionsUtil(Class<? extends RelNode> clazz, RelBuilderFactory relBuilderFactory, String desc) {
    super(clazz, relBuilderFactory, desc);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    return;
  }

  public static boolean numReducibleExprs(Filter filter) {
    final List<RexNode> expList = Lists.newArrayList(filter.getCondition());
    final RelMetadataQuery mq = RelMetadataQuery.instance(DefaultRelMetadataProvider.INSTANCE);
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(filter.getInput());
    return reduceExpressions(filter, expList, predicates);
  }

  public static int numReducibleExprs(Project project) {
    final List<RexNode> expList = Lists.newArrayList(project.getProjects());
    final RelMetadataQuery mq = RelMetadataQuery.instance(DefaultRelMetadataProvider.INSTANCE);
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(project.getInput());
    boolean reducible = reduceExpressions(project, expList, predicates);
    if (reducible) {
      int numReducible = 0;
      for (int i = 0; i < project.getProjects().size(); i++) {
        if (!project.getProjects().get(i).toString().equals(expList.get(i).toString())) {
          numReducible++;
        }
      }
      return numReducible;
    } else {
      return 0;
    }
  }
}
