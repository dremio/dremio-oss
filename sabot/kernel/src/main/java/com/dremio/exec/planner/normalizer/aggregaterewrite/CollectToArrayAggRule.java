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
package com.dremio.exec.planner.normalizer.aggregaterewrite;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;

/**
 * This rule is for rewriting ARRAY subquery into ARRAY_AGG.
 *
 * <p>For example:
 *
 * <p>SELECT ARRAY(SELECT ENAME FROM EMP WHERE DEPTNO = 20)
 *
 * <p>Get's rewritten to:
 *
 * <p>SELECT ARRAY_AGG(ENAME) FROM EMP WHERE DEPTNO = 20
 *
 * <p>If we want to talk about RelNodes then the plans are:
 *
 * <p>Collect(field=[EXPR$0]) LogicalProject(ENAME=[$1]) LogicalFilter(condition=[=($6, 20)])
 * ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,
 * `DEPTNO`, `COMM`], splits=[1])
 *
 * <p>And:
 *
 * <p>LogicalAggregate(group=[{}], collect_to_array_agg=[ARRAY_AGG($0)]) LogicalProject(ENAME=[$1])
 * LogicalFilter(condition=[=($6, 20)]) ScanCrel(table=[cp.scott."EMP.json"], columns=[`EMPNO`,
 * `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `DEPTNO`, `COMM`], splits=[1])
 */
public final class CollectToArrayAggRule extends RelRule<CollectToArrayAggRule.Config>
    implements TransformationRule {
  public static final CollectToArrayAggRule INSTANCE =
      CollectToArrayAggRule.Config.DEFAULT.toRule();

  private CollectToArrayAggRule(CollectToArrayAggRule.Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Collect collect = call.rel(0);
    RelNode input = collect.getInput();
    RelDataType inputRowType = input.getRowType();
    if (inputRowType.getFieldCount() != 1) {
      throw UserException.planError()
          .message("ARRAY Subquery can only support 1 column in the projection.")
          .buildSilently();
    }

    RelDataType elementType = inputRowType.getFieldList().get(0).getType();

    AggregateCall arrayAgg =
        AggregateCall.create(
            DremioSqlOperatorTable.ARRAY_AGG,
            false,
            false,
            false,
            ImmutableList.of(0),
            -1,
            RelCollations.EMPTY,
            0,
            collect.getInput(),
            SqlTypeFactoryImpl.INSTANCE.createTypeWithNullability(
                SqlTypeFactoryImpl.INSTANCE.createArrayType(elementType, -1), false),
            "collect_to_array_agg");
    RelNode rewrittenRelNode =
        call.builder()
            .push(collect.getInput())
            .aggregate(call.builder().groupKey(), ImmutableList.of(arrayAgg))
            .build();

    call.transformTo(rewrittenRelNode);
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("CollectToAggregateRule")
            .withOperandSupplier(op -> op.operand(Collect.class).anyInputs())
            .as(Config.class);

    @Override
    default CollectToArrayAggRule toRule() {
      return new CollectToArrayAggRule(this);
    }
  }
}
