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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

public abstract class NullableFunctionConvertlet implements FunctionConvertlet {
  private final FunctionConvertlet innerConvertlet;

  public NullableFunctionConvertlet(FunctionConvertlet innerConvertlet) {
    Preconditions.checkNotNull(innerConvertlet);
    this.innerConvertlet = innerConvertlet;
  }

  public abstract RexNode whenReturnNull(final RexBuilder rexBuilder, final RexCall originalCall);

  @Override
  public boolean matches(RexCall call) {
    return innerConvertlet.matches(call);
  }

  @Override
  public RexCall convertCall(
    ConvertletContext cx,
    RexCall call) {
    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode when = whenReturnNull(rexBuilder, call);
    RexCall innerRewrite = innerConvertlet.convertCall(cx, call);
    if (when.isAlwaysFalse()) {
      return innerRewrite;
    }

    RexNode nullLiteral = rexBuilder.makeNullLiteral(call.getType());
    return (RexCall) rexBuilder.makeCall(CASE, when, nullLiteral, innerRewrite);
  }
}
