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
package com.dremio.exec.planner.sql.evaluator;

import com.dremio.sabot.exec.context.ContextInformation;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/** Evaluator for the CURRENT_SCHEMA function. */
public final class CurrentSchemaEvaluator implements FunctionEval {
  public static final CurrentSchemaEvaluator INSTANCE = new CurrentSchemaEvaluator();

  private CurrentSchemaEvaluator() {}

  @Override
  public RexNode evaluate(EvaluationContext cx, RexCall call) {
    ContextInformation contextInformation = cx.getContextInformation();
    RexBuilder rexBuilder = cx.getRexBuilder();
    RexNode result = rexBuilder.makeLiteral(contextInformation.getCurrentDefaultSchema());
    RexNode matchedPrecision = rexBuilder.makeAbstractCast(call.getType(), result);

    return matchedPrecision;
  }
}
