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

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.REGEXP_LIKE;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * "REGEXP_LIKE is similar to the LIKE condition, except REGEXP_LIKE performs regular expression
 * matching instead of the simple pattern matching performed by LIKE."
 *
 * <p>We noticed that for simple patterns LIKE performs about twice as fast as REGEXP_LIKE. This is
 * most likely due to the fact that LIKE is running less code, since it has a simpler pattern
 * language.
 *
 * <p>Since we don't have access to either implementation we can instead write a rule to use LIKE in
 * place of REGEXP_LIKE in certain scenarios.
 *
 * <p>REGEXP_LIKE has the following special characters +, *, ?, ^, $, (, ), [, ], {, }, |, \ which
 * means that none of the other characters have special meaning. If a pattern doesn't have any
 * special characters, then we can safely convert REGEXP_LIKE to LIKE.
 *
 * <p>For example:
 *
 * <p>SELECT * FROM EMP WHERE REGEXP_LIKE(name, 'asdf')
 *
 * <p>Can we rewritten to:
 *
 * <p>SELECT * FROM EMP WHERE LIKE(name, '%asdf%')
 */
public final class RegexpLikeToLikeConvertlet implements FunctionConvertlet {
  public static final RegexpLikeToLikeConvertlet INSTANCE = new RegexpLikeToLikeConvertlet();

  private RegexpLikeToLikeConvertlet() {}

  private static final Set<Character> SPECIAL_CHARACTERS =
      ImmutableSet.of('.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\');

  @Override
  public boolean matches(RexCall call) {
    // If the right operand is not a string literal or
    // if the pattern has special characters, then don't do a rewrite.
    if (call.getOperator() != REGEXP_LIKE) {
      return false;
    }

    if (!call.getOperands().get(1).isA(SqlKind.LITERAL)) {
      return false;
    }

    String pattern = ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class);
    for (int i = 0; i < pattern.length(); i++) {
      Character patternCharacter = pattern.charAt(i);
      if (SPECIAL_CHARACTERS.contains(patternCharacter)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public RexCall convertCall(ConvertletContext cx, RexCall call) {
    String pattern = ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class);
    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode sourceString = call.getOperands().get(0);
    RexNode newPattern = rexBuilder.makeLiteral("%" + pattern + "%");
    return (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LIKE, sourceString, newPattern);
  }
}
