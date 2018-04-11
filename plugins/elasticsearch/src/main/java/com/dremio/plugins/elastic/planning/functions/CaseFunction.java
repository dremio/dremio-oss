/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.plugins.elastic.planning.functions;

import java.util.List;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

class CaseFunction extends ElasticFunction {

  public CaseFunction(){
    super("case", "case");
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    Preconditions.checkArgument(call.getOperands().size() % 2 == 1,
        "Number of arguments to a case function should be odd.");
    return handleCaseFunctionHelper(renderer, call.getOperands(), 0);
  }

  private FunctionRender handleCaseFunctionHelper(final FunctionRenderer renderer, final List<RexNode> operands, final int start) {
    if (start == operands.size() - 1) {
      return operands.get(start).accept(renderer.getVisitor());
    }

    final FunctionRender startRender = operands.get(start).accept(renderer.getVisitor());
    final FunctionRender startPlusRender = operands.get(start+1).accept(renderer.getVisitor());
    final FunctionRender startPlusTwoRender = handleCaseFunctionHelper(renderer, operands, start + 2);

    /*
     * DX-8047: If using painless, we need to make sure that we cast arguments on either side of an if condition to object types.
     */
    final String substitutionString = renderer.isUsingPainless() ?
        "( ( %s ) ? (def) ( %s ) : (def) ( %s ) )" :
        "( ( %s ) ? ( %s ) : ( %s ) )";

    /*
     * DX-10765: If there is an ELSE clause (like a default in switch), then the
     * last expression is the NULL literal. In this case, do not guard against
     * the value being null. The last expression must be returned. The last
     * expression itself may be the NULL literal, which is also correctly handled.
     */
    final RexNode lastNode = Util.last(operands);
    final boolean isLastNullLiteral = start == 0
        && (lastNode instanceof RexLiteral)
        && ((RexLiteral) lastNode).getTypeName().equals(SqlTypeName.NULL);

    /*
     * The nullabilty of the condition should be checked by the parent
     * expression.
     *
     * The nullability of the if and else branches should be evaluated inside
     * the return to avoid returning null on a irrelevant side of the branch.
     */
    final String newScript = String.format(
        substitutionString,
        startRender.getScript(),
        startPlusRender.getNullGuardedScript(),
        startPlusTwoRender.getNullGuardedScript());

    return new FunctionRender(newScript, startRender.getNulls()) {
      @Override
      public String getNullGuardedScript() {
        if (isLastNullLiteral) {
          return super.getNullGuardedScript();
        } else {
          return getScript();
        }
      }
    };
  }

}
