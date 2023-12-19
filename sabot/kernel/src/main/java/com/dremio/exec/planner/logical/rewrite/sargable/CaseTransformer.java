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
package com.dremio.exec.planner.logical.rewrite.sargable;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

/**
 * <pre>
 * Transform the following two cases
 * 1. COALESCE(ts1, ts2, ts3) = '2023-02-01'
 * 2. CASE WHEN ts1 IS NOT NULL THEN ts1 = '2023-02-01'
 *          WHEN ts2 IS NOT NULL THEN ts2 = '2023-02-01'
 *          ELSE ts3 = '2023-02-01'
 *    END
 * to
 *  (ts1 IS NOT NULL AND ts1 = '2023-02-01') OR
 *  (ts2 IS NOT NULL AND ts2 = '2023-02-01') OR
 *  (ts3 = '2023-02-01')
 *  </pre>
 */
public class CaseTransformer implements Transformer {

  private final RexBuilder rexBuilder;
  private final StandardForm standardForm;

  CaseTransformer(RelOptCluster relOptCluster,
                  StandardForm standardForm) {
    this.rexBuilder = relOptCluster.getRexBuilder();
    this.standardForm = standardForm;
  }

  @Override
  public RexNode transform() {
    ImmutableList<RexNode> ops = getLhsCall().operands;
    List<RexNode> nodes = new ArrayList<>();
    if (ops.size() <= 1) {
      return getLhsCall();
    }
    for (int i = 0; i < ops.size() - 1; i += 2) {
      if (!ops.get(i).getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN) ||
        !ops.get(i + 1).getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
        return null;
      }
      if (i == 0) {
        nodes.add(SARGableRexUtils.and(ops.get(i), ops.get(i + 1), rexBuilder));
      } else {
        nodes.add(SARGableRexUtils.and(ImmutableList.of(addNot(ops.get(i - 2)), ops.get(i), ops.get(i + 1)), rexBuilder));
      }
    }
    if (ops.size() % 2 == 1) {
      nodes.add(SARGableRexUtils.and(addNot(ops.get(ops.size() - 3)), ops.get(ops.size() - 1), rexBuilder));
    }
    if (nodes.size() == 1) {
      return nodes.get(0);
    }
    return SARGableRexUtils.or(nodes, rexBuilder);
  }

  RexCall getLhsCall() {
    return standardForm.getLhsCall();
  }

  private RexNode addNot(RexNode node) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      if (call.op.equals(SqlStdOperatorTable.IS_NOT_NULL)) {
        return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, call.operands);
      }
    }
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, node);
  }
}
