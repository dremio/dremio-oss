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

import com.dremio.plugins.elastic.planning.rules.PredicateAnalyzer;
import com.google.common.collect.Iterables;


class BinaryFunction extends ElasticFunction {

  public BinaryFunction(String commonName){
    super(commonName, commonName);
  }

  public BinaryFunction(String dremioName, String elasticName){
    super(dremioName, elasticName);
  }

  /**
   * Convert a Rex Call for a binary operation into a groovy script.
   * Note that these expressions can have more than two inputs and represent
   * a chain of binary operations.
   *
   * Example:
   * a = 5 OR b = 10 OR c = 20
   *
   * Can be represented as a single RexCall for OR with three child expressions.
   *
   * @param groovyOp the string of the groovy operation that this SQL function maps to
   * @param call the function to convert
   * @return a string containging a groovy script
   */
  @Override
  public FunctionRender render(FunctionRenderer renderer, RexCall call) {
    checkArity(call, 2);
    PredicateAnalyzer.checkForIncompatibleDateTimeOperands(call);

    FunctionRender op1 = call.getOperands().get(0).accept(renderer.getVisitor());
    FunctionRender op2 = call.getOperands().get(1).accept(renderer.getVisitor());

    String script = String.format("( %s %s %s )", op1.getScript(), elasticName, op2.getScript());
    return new FunctionRender(script, Iterables.concat(op1.getNulls(), op2.getNulls()));
  }

}
