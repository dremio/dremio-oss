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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public final class OverUtils {

  private OverUtils() {
  }

  /**
   * Indicates if the given OVER clause has a window frame that is automatically added by
   * Calcite if the frame is not specified.
   */
  public static boolean hasDefaultFrame(RexOver over) {
    // When Calcite parses an OVER clause with no frame,
    // it inject a 'default' frame depending on the function.
    // 1. For ROW_NUMBER(), it generates ROWS UNBOUNDED PRECEDING and CURRENT ROW.
    // 2. For others, it generates RANGE UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING if unsorted.
    // 3. If it's not ROW_NUMBER(), and it is sorted, Calcite uses RANGE UNBOUNDED PRECEDING AND CURRENT ROW
    // Adding these unnecessary frames cause some RDBMSes (eg SQL Server) to fail.
    //
    // This code happens in SqlToRelConverter.convertOver(), SqlValidatorImpl.resolveWindow(),
    // and SqlWindow.create()/SqlWindow.populateBounds().

    return // Note: intentionally not simplifying this boolean for clarity.
      (over.getAggOperator() == SqlStdOperatorTable.ROW_NUMBER && over.getWindow().isRows() &&
        over.getWindow().getLowerBound().isUnbounded() && over.getWindow().getLowerBound().isPreceding() &&
        over.getWindow().getUpperBound().isCurrentRow()) // First condition.
        ||
        (!over.getWindow().isRows() && over.getWindow().orderKeys.isEmpty() &&
          over.getWindow().getLowerBound().isUnbounded() && over.getWindow().getLowerBound().isPreceding() &&
          over.getWindow().getUpperBound().isUnbounded() && over.getWindow().getUpperBound().isFollowing())  // Second condition.
        ||
        (!over.getWindow().isRows() && !over.getWindow().orderKeys.isEmpty() &&
          over.getWindow().getLowerBound().isUnbounded() && over.getWindow().getLowerBound().isPreceding() &&
          !over.getWindow().getUpperBound().isUnbounded() && over.getWindow().getUpperBound().isCurrentRow());  // Third condition.
  }
}
