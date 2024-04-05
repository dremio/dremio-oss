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
package com.dremio.plugins.elastic.planning.functions;

import org.apache.calcite.rex.RexCall;

public class LogFunction extends ElasticFunction {

  public LogFunction(String dremioName, String elasticName) {
    super(dremioName, elasticName);
  }

  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 2);
    FunctionRender operand0 = call.getOperands().get(0).accept(renderer.getVisitor());
    FunctionRender operand1 = call.getOperands().get(1).accept(renderer.getVisitor());
    return new FunctionRender(
        String.format("( log(%s)/log(%s) )", operand0.getScript(), operand1.getScript()),
        nulls(operand0, operand1));
  }
}
