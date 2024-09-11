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

import static org.apache.calcite.sql.SqlKind.ARRAY_QUERY_CONSTRUCTOR;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableList;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

public final class DremioArraySubQueryRemoveRule extends SubQueryRemoveRule {

  /**
   * Creates a SubQueryRemoveRule.
   *
   * @param config
   */
  public DremioArraySubQueryRemoveRule(Config config) {
    super(config);
  }

  @Override
  protected RexNode apply(
      RexSubQuery e,
      Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic,
      RelBuilder builder,
      int inputCount,
      int offset) {
    if (e.getKind() != ARRAY_QUERY_CONSTRUCTOR) {
      return super.apply(e, variablesSet, logic, builder, inputCount, offset);
    }

    return rewriteArraySubquery(e, variablesSet, builder, inputCount, offset);
  }

  private static RexNode rewriteArraySubquery(
      RexSubQuery e,
      Set<CorrelationId> variablesSet,
      RelBuilder builder,
      int inputCount,
      int offset) {
    assert e.getKind() == ARRAY_QUERY_CONSTRUCTOR;

    builder
        .push(e.rel)
        .push(Collect.create(builder.build(), e.getKind(), "x"))
        .join(JoinRelType.LEFT, builder.literal(true), variablesSet);
    RexNode array = field(builder, inputCount, offset);

    RexBuilder rexBuilder = builder.getRexBuilder();
    // This is a hack to make the nullability match,
    // But in the future it needs to return an empty array as per the spec.
    RelDataType nonNullable =
        builder.getTypeFactory().createTypeWithNullability(array.getType(), false);
    RexNode emptyArray = rexBuilder.makeCall(DremioSqlOperatorTable.EMPTY_ARRAY, array);
    RexNode isNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, array);
    return rexBuilder.makeCall(
        nonNullable, SqlStdOperatorTable.CASE, ImmutableList.of(isNull, emptyArray, array));
  }

  private static RexInputRef field(RelBuilder builder, int inputCount, int offset) {
    for (int inputOrdinal = 0; ; ) {
      final RelNode r = builder.peek(inputCount, inputOrdinal);
      if (offset < r.getRowType().getFieldCount()) {
        return builder.field(inputCount, inputOrdinal, offset);
      }
      ++inputOrdinal;
      offset -= r.getRowType().getFieldCount();
    }
  }
}
