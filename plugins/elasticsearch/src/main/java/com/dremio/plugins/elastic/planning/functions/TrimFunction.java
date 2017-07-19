/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;

class TrimFunction extends ElasticFunction {

  private static final String TRIM_CHAR = "' '";

  public TrimFunction(){
    super("trim", "trim");
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 3);

    RexNode op0 = call.getOperands().get(0);
    if (op0 instanceof RexLiteral) {
      final FunctionRender trimChar = call.getOperands().get(1).accept(renderer.getVisitor());
      final FunctionRender inputStr = call.getOperands().get(2).accept(renderer.getVisitor());
      if (TRIM_CHAR.equals(trimChar.getScript())) {
        if (((RexLiteral) op0).getValue() == SqlTrimFunction.Flag.BOTH) {
          return new FunctionRender(inputStr.getScript() + ".trim()", inputStr.getNulls());
        }
      }
    }
    throw new UnsupportedOperationException("incorrect arguments for trim function");
  }
}
