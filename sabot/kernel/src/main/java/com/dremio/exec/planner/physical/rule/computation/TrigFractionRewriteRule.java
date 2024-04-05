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
package com.dremio.exec.planner.physical.rule.computation;

import com.dremio.exec.planner.logical.RexRewriter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

class TrigFractionRewriteRule extends RexRewriter.RewriteRule {

  public TrigFractionRewriteRule(RexBuilder builder) {
    super(builder);
  }

  @Override
  public RexNode rewrite(RexCall call) {
    switch (call.getOperator().getName().toUpperCase()) {
      case "SIN":
      case "COS":
      case "TAN":
        if (!(call.getOperands().get(0) instanceof RexCall)) {
          return null;
        }
        RexCall child = (RexCall) call.getOperands().get(0);
        switch (child.getOperator().getKind()) {
          case DIVIDE:
            if (!(RexUtil.isConstant(child.getOperands().get(1))
                && child.getOperands().get(0) instanceof RexCall)) {
              return null;
            }
            RexCall gChild = (RexCall) child.getOperands().get(0);
            if (gChild.getOperands().size() != 2) {
              return null;
            }
            switch (gChild.getOperator().getKind()) {
              case PLUS:
              case MINUS:
                return builder.makeCall(
                    call.getOperator(),
                    builder.makeCall(
                        gChild.getOperator(),
                        builder.makeCall(
                            child.getOperator(),
                            gChild.getOperands().get(0),
                            child.getOperands().get(1)),
                        builder.makeCall(
                            child.getOperator(),
                            gChild.getOperands().get(1),
                            child.getOperands().get(1))));
              default:
                return null;
            }
          default:
            return null;
        }
      default:
        return null;
    }
  }
}
