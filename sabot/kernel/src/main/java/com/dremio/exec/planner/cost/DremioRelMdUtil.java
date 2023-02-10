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
package com.dremio.exec.planner.cost;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.expr.RexTableColumnNameRef;
import com.dremio.exec.planner.physical.PrelUtil;

/**
 * DremioRelMdUtil
 */
public class DremioRelMdUtil {
  public static boolean isStatisticsEnabled(RelOptPlanner planner, boolean isNoOp) {
    return !isNoOp && (PrelUtil.getPlannerSettings(planner).useStatistics() || PrelUtil.getPlannerSettings(planner).semiJoinCosting());
  }

  public static boolean isRowCountStatisticsEnabled(RelOptPlanner planner, boolean isNoOp) {
    return !isNoOp && PrelUtil.getPlannerSettings(planner).useRowCountStatistics();
  }

  /**
   * Given an expression, it will swap the table references contained in its
   * {@link RexTableInputRef} to a new {@link RexTableColumnNameRef}  which will wrap It
   */
  public static RexNode swapTableInputReferences(final RexNode node) {
    RexShuttle visitor =
      new RexShuttle() {
        @Override public RexNode visitTableInputRef(RexTableInputRef inputRef) {
          return new RexTableColumnNameRef(inputRef);
        }
      };
    return visitor.apply(node);
  }

  public static String getNameFromColumnOrigin(RelColumnOrigin columnOrigin){
    String columnNameWithTableName = PathUtils.constructFullPath(columnOrigin.getOriginTable().getQualifiedName()) +
      "."+ columnOrigin.getOriginTable().getRowType().getFieldNames().get(columnOrigin.getOriginColumnOrdinal());
    return columnNameWithTableName;
  }

}
