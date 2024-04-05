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

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.HashToRandomExchangePrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.RoundRobinExchangePrel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.exec.planner.physical.WindowPrel;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

/**
 * A rule that applies additional sort-window-filter operator below exchange.
 *
 * <p>e.g.
 *
 * <p>(1) Filter($2 <= 100) (2) Window(partition_by=$0, order_by=$1, window_function=rank()) (3)
 * Sort($1) (4) HashToRandomExchange($0)
 *
 * <p>is converted to
 *
 * <p>(1) Filter($2 <= 100) (2) Window(partition_by=$0, order_by=$1, window_function=rank()) (3)
 * Sort($1) (4) HashToRandomExchange($0) (*5) Filter($2 <= 100) (*6) Window(partition_by=$0,
 * order_by=$1, window_function=rank()) (*7) Sort($1) (*8) RoundRobinExchange()
 *
 * <p>In the original plan, if there is only a few distinct partition values, the input for each
 * "(3) sort" operator would be very large. To reduce burden on this operator, we could apply
 * another set of sort-window-filter operator below exchange. In this way, we could do (*7)sort in a
 * fully distributed way and filter out some unnecessary result from the first run.
 */
public class AddFilterWindowBelowExchangeRule extends RelOptRule {
  private static final Set<SqlKind> SUPPORTED_WINDOW_FUNCTIONS =
      ImmutableSet.of(SqlKind.RANK, SqlKind.DENSE_RANK, SqlKind.ROW_NUMBER, SqlKind.COUNT);

  public static final RelOptRule INSTANCE = new AddFilterWindowBelowExchangeRule();

  private AddFilterWindowBelowExchangeRule() {
    super(
        RelOptHelper.some(
            FilterPrel.class,
            RelOptHelper.some(
                WindowPrel.class,
                RelOptHelper.some(
                    SortPrel.class, RelOptHelper.any(HashToRandomExchangePrel.class)))),
        "AddFilterWindowBelowExchangeRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(call.getPlanner());
    final FilterPrel filter = call.rel(0);
    final WindowPrel window = call.rel(1);
    final SortPrel sort = call.rel(2);
    final HashToRandomExchangePrel exchange = call.rel(3);

    return plannerSettings.getOptions().getOption(PlannerSettings.ENABLE_SORT_ROUND_ROBIN)
        && !exchange.isWindowPushedDown()
        && checkWindow(window)
        && checkFilter(
            filter,
            sort.getRowType().getFieldCount(),
            window.getRowType().getFieldCount(),
            (int) plannerSettings.getMaxCnfNodeCount());
  }

  private boolean checkFilter(
      FilterPrel filterPrel, int incomingFieldCount, int windowFieldCount, int maxCnfNodeCount) {
    final RexNode cnfCondition =
        RexUtil.toCnf(
            filterPrel.getCluster().getRexBuilder(), maxCnfNodeCount, filterPrel.getCondition());

    for (RexNode condition : conjunctions(cnfCondition)) {
      if (condition instanceof RexCall) {
        final RexCall call = (RexCall) condition;
        switch (call.getOperator().getKind()) {
          case LESS_THAN:
          case LESS_THAN_OR_EQUAL:
            if (checkCall(call, false, incomingFieldCount, windowFieldCount)) {
              return true;
            }
            break;
          case GREATER_THAN:
          case GREATER_THAN_OR_EQUAL:
            if (checkCall(call, true, incomingFieldCount, windowFieldCount)) {
              return true;
            }
            break;
          default:
            break;
        }
      }
    }
    return false;
  }

  private boolean checkCall(
      RexCall call, boolean reverse, int incomingFieldCount, int windowFilterCount) {
    RexNode op1 = call.getOperands().get(0);
    RexNode op2 = call.getOperands().get(1);
    if (reverse) {
      RexNode temp = op1;
      op1 = op2;
      op2 = temp;
    }
    if (!(op1 instanceof RexInputRef && op2 instanceof RexLiteral)) {
      return false;
    }
    int index = ((RexInputRef) op1).getIndex();
    return index >= incomingFieldCount && index < windowFilterCount;
  }

  private boolean checkWindow(WindowPrel window) {
    for (Window.Group group : window.groups) {
      if (group.keys.cardinality() == 0
          || group.orderKeys.getFieldCollations().isEmpty()
          || !group.aggCalls.stream()
              .allMatch(call -> SUPPORTED_WINDOW_FUNCTIONS.contains(call.getKind()))) {
        return false;
      }
    }
    if (window.groups.size() != 1) {
      return false;
    }

    Window.Group group = window.groups.get(0);
    for (Window.RexWinAggCall call : group.aggCalls) {
      if (!SUPPORTED_WINDOW_FUNCTIONS.contains(call.op.kind)) {
        return false;
      }
      if (call.op.kind == SqlKind.COUNT) {
        if (!((group.lowerBound.isUnbounded() && !group.upperBound.isUnbounded())
            || (group.upperBound.isUnbounded() && !group.lowerBound.isUnbounded()))) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterPrel filter = call.rel(0);
    final WindowPrel window = call.rel(1);
    final SortPrel sort = call.rel(2);
    final HashToRandomExchangePrel exchange = call.rel(3);

    final RelTraitSet relTraitSet =
        exchange.getInput().getTraitSet().plus(DistributionTrait.ROUND_ROBIN);
    final RoundRobinExchangePrel roundRobinExchange =
        new RoundRobinExchangePrel(
            exchange.getInput().getCluster(), relTraitSet, exchange.getInput());
    final SortPrel newSort =
        (SortPrel) sort.copy(sort.getTraitSet(), Collections.singletonList(roundRobinExchange));
    final WindowPrel bottomWindow =
        (WindowPrel) window.copy(window.getTraitSet(), Collections.singletonList(newSort));
    final FilterPrel bottomFilter =
        (FilterPrel) filter.copy(bottomWindow.getTraitSet(), bottomWindow, filter.getCondition());
    final List<RexNode> identityProjects =
        new ArrayList<>(sort.getCluster().getRexBuilder().identityProjects(exchange.getRowType()));
    final ProjectPrel identityProject =
        ProjectPrel.create(
            bottomFilter.getCluster(),
            bottomFilter.getTraitSet(),
            bottomFilter,
            identityProjects,
            exchange.getRowType());

    final HashToRandomExchangePrel exchangePrel =
        (HashToRandomExchangePrel)
            exchange.copy(exchange.getTraitSet(), Collections.singletonList(identityProject), true);
    final SortPrel topSort =
        (SortPrel) sort.copy(sort.getTraitSet(), Collections.singletonList(exchangePrel));
    final WindowPrel topWindow =
        (WindowPrel) window.copy(window.getTraitSet(), Collections.singletonList(topSort));
    final FilterPrel topFilter =
        (FilterPrel) filter.copy(filter.getTraitSet(), topWindow, filter.getCondition());

    call.transformTo(topFilter);
  }
}
