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
package org.apache.calcite.rex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

/**
 * Util class for Over
 */
public class WindowUtil {

  private WindowUtil() {}

  public static List<RexOver> getOver(Window w) {
    RelNode input = w.getInput();
    int inputFieldCount = input.getRowType().getFieldCount();
    final List<RexOver> rexOvers = new ArrayList<>();
    for (Window.Group group : w.groups) {
      final List<RexNode> partitionKeys = new ArrayList<>();
      final List<RexFieldCollation> orderKeys = new ArrayList<>();

      // Convert RelFieldCollation to RexFieldCollation
      for (RelFieldCollation collation : group.orderKeys.getFieldCollations()) {
        Set<SqlKind> directions = new HashSet<>();
        if (collation.direction.isDescending()) {
          directions.add(SqlKind.DESCENDING);
        }
        if (collation.nullDirection == RelFieldCollation.NullDirection.LAST) {
          directions.add(SqlKind.NULLS_LAST);

        } else if (collation.nullDirection == RelFieldCollation.NullDirection.FIRST) {
          directions.add(SqlKind.NULLS_FIRST);
        }

        RexFieldCollation rexCollation = new RexFieldCollation(w.getCluster().getRexBuilder().makeInputRef(input, collation.getFieldIndex()), directions);
        orderKeys.add(rexCollation);
      }

      // Add partition keys
      for (int partition : group.keys) {
        partitionKeys.add(w.getCluster().getRexBuilder().makeInputRef(input, partition));
      }

      // Create RexWindow
      RexWindow window = new RexWindow(partitionKeys, orderKeys, group.lowerBound, group.upperBound, group.isRows);

      // For each window agg call, create rex over
      for (Window.RexWinAggCall winAggCall : group.aggCalls) {

        RexShuttle replaceConstants = new RexShuttle() {
          @Override public RexNode visitInputRef(RexInputRef inputRef) {
            int index = inputRef.getIndex();
            RexNode ref;
            if (index > inputFieldCount - 1) {
              ref = w.constants.get(index - inputFieldCount);
            } else {
              ref = inputRef;
            }
            return ref;
          }
        };
        RexCall aggCall = (RexCall) winAggCall.accept(replaceConstants);
        SqlAggFunction aggFunction = (SqlAggFunction) winAggCall.getOperator();
        RexOver over = new RexOver(winAggCall.getType(), aggFunction, aggCall.operands, window, winAggCall.distinct);
        rexOvers.add(over);
      }
    }
    return rexOvers;
  }

}
