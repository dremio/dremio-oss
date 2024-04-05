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
package com.dremio.exec.planner.sql.convertlet;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.COL_LIKE;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.REGEXP_COL_LIKE;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.REGEXP_LIKE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LIKE;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * Rewrites `LIKE` operator into `COL_LIKE` function when both operands are column identifiers. Also
 * rewrites 'REGEXP_LIKE' function call into 'REGEXP_COL_LIKE' function call using the same logic.
 *
 * <p>Example:
 *
 * <p>SELECT * FROM TEST_TABLE WHERE A LIKE B
 *
 * <p>is rewritten to
 *
 * <p>SELECT * FROM TEST_TABLE WHERE COL_LIKE(A, B)
 *
 * <p>SELECT * FROM TEST_TABLE WHERE REGEXP_LIKE(A, B)
 *
 * <p>is rewritten to
 *
 * <p>SELECT * FROM TEST_TABLE WHERE REGEXP_COL_LIKE(A, B)
 *
 * <p>
 */
public final class LikeToColumnLikeConvertlet implements FunctionConvertlet {
  public static final LikeToColumnLikeConvertlet LIKE_TO_COL_LIKE =
      new LikeToColumnLikeConvertlet(LIKE, COL_LIKE);
  public static final LikeToColumnLikeConvertlet REGEXP_LIKE_TO_REGEXP_COL_LIKE =
      new LikeToColumnLikeConvertlet(REGEXP_LIKE, REGEXP_COL_LIKE);

  private final SqlOperator src;
  private final SqlOperator dst;

  private LikeToColumnLikeConvertlet(SqlOperator src, SqlOperator dst) {
    this.src = src;
    this.dst = dst;
  }

  @Override
  public boolean matches(RexCall call) {
    return call.getOperator().equals(src)
        && call.getOperands().get(0).isA(SqlKind.INPUT_REF)
        && call.getOperands().get(1).isA(SqlKind.INPUT_REF);
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    // Replace with src SqlOperator if operands are column refs
    final RexNode left = call.getOperands().get(0);
    final RexNode right = call.getOperands().get(1);
    final RexBuilder rexBuilder = cx.getRexBuilder();

    return (RexCall) rexBuilder.makeCall(dst, left, right);
  }
}
