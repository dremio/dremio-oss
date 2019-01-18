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
package com.dremio.exec.resolver;

import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.google.common.collect.Lists;

public class ExactFunctionResolver implements FunctionResolver {

  /*
   * This function resolves the input call to a func holder only if all
   * the input argument types match exactly with the func holder arguments. This is used when we
   * are trying to inject an implicit cast and do not want to inject another implicit
   * cast
   */
  @Override
  public BaseFunctionHolder getBestMatch(List<BaseFunctionHolder> methods, FunctionCall call) {

    int currcost;

    for (BaseFunctionHolder h : methods) {
      final List<CompleteType> argumentTypes = Lists.newArrayList();
      for (LogicalExpression expression : call.args) {
        argumentTypes.add(expression.getCompleteType());
      }
      currcost = TypeCastRules.getCost(argumentTypes, h);

      // Return if we found a function that has an exact match with the input arguments
      if (currcost  == 0){
        return h;
      }
    }
    // No match found
    return null;
  }
}
