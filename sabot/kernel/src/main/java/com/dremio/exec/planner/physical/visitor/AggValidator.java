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

package com.dremio.exec.planner.physical.visitor;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.ARRAY_AGG;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;

import com.dremio.common.expression.ListAggExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.physical.AggregatePrel;
import com.dremio.exec.planner.physical.Prel;
import com.google.common.base.Preconditions;

public final class AggValidator extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private static final AggValidator INSTANCE = new AggValidator();
  public static Prel validate(Prel prel) {
    return prel.accept(INSTANCE, null);
  }

  private void validateAggregateExpression(LogicalExpression agg) {
    if (agg instanceof ListAggExpression) {
      ListAggExpression lAgg = (ListAggExpression) agg;
      Preconditions.checkArgument(!ARRAY_AGG.getName().equals(lAgg.getName()) || !lAgg.isDistinct(),
        "Failure while planning to query. Please remove ARRAY_AGG(DISTINCT) and try again."
        );
      Preconditions.checkArgument(!ARRAY_AGG.getName().equals(lAgg.getName()) || lAgg.getOrderings() == null ||
          lAgg.getOrderings().isEmpty(),
        "Failure while planning to query. Please remove ARRAY_AGG and try again."
      );
      Preconditions.checkArgument(!ARRAY_AGG.getName().equals(lAgg.getName()) || lAgg.getExtraExpressions() == null ||
          lAgg.getExtraExpressions().isEmpty(),
        "Failure while planning to query. Please remove ARRAY_AGG and try again."
      );
    }
  }

  private void validateAggCall(AggregateCall aggCall) {
    if (ARRAY_AGG.equals(aggCall.getAggregation())) {
      Preconditions.checkArgument(!aggCall.isDistinct(),
        "Failure while planning to query. Please remove ARRAY_AGG(DISTINCT) and try again."
      );
      Preconditions.checkArgument(!aggCall.isApproximate(),
        "Failure while planning to query. Please remove ARRAY_AGG and try again.");
      Preconditions.checkArgument(!aggCall.hasFilter(),
        "Failure while planning to query. Please remove ARRAY_AGG and try again.");
      Preconditions.checkArgument(aggCall.getCollation().getFieldCollations().isEmpty(),
        "Failure while planning to query. Please remove ARRAY_AGG and try again.");
    }
  }

  @Override
  public Prel visitAggregate(AggregatePrel prel, Void voidValue) throws RuntimeException {
    prel.getAggCallList().forEach(this::validateAggCall);
    return super.visitAggregate(prel, voidValue);
  }

  @Override
  public Prel visitPrel(Prel prel, Void voidValue) throws RuntimeException {
    List<RelNode> children = new ArrayList<>();

    for (Prel child : prel) {
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
