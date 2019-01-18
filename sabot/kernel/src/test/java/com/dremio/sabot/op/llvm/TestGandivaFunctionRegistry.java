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
package com.dremio.sabot.op.llvm;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionRegistry;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestGandivaFunctionRegistry extends ExecTest {

  private static int totalFuncs = 0, unSupportedFn = 0;
  /*
   * Test that function lookups on the gandiva repository works.
   */
  @Test
  public void testGandivaPluggableRegistry() throws Exception {
    GandivaFunctionRegistry fnRegistry = new GandivaFunctionRegistry(DEFAULT_SABOT_CONFIG);

    FunctionCall fnCall = getAddFn();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.getFunction(fnCall);
    Assert.assertNotNull(holder);
    ArrowType.Int int32 = new ArrowType.Int(32, true);
    CompleteType expectedReturnType = new CompleteType(int32);
    Assert.assertEquals(expectedReturnType, holder.getReturnType());
    Assert.assertNotNull(holder.getExpr(fnCall.getName(), fnCall.args));
  }

  /*
   * Test that dremio repository is integrated with Gandiva as a pluggable repository/
   */
  @Test
  public void testFunctionImplementationRegistry() {
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.findNonFunction(getAddFn());
    Assert.assertNotNull(holder);
  }

  @Test
  public void getUnSupportedFunctions() throws GandivaException {
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    ArrayListMultimap<String, BaseFunctionHolder> functions = fnRegistry.getRegisteredFunctions();
    Set<String> fns = Sets.newHashSet();
    Set<FunctionSignature> supportedFunctions = ExpressionRegistry.getInstance()
      .getSupportedFunctions();
    for (FunctionSignature signature : supportedFunctions ) {
      String fnName = (signature.getName().toLowerCase() +"##");
      for (ArrowType param : signature.getParamTypes()) {
        fnName = fnName + "##" + param.toString();
      }
      fns.add(fnName);
    }
    for (Map.Entry<String, BaseFunctionHolder> holders : functions.entries()) {
      String name = holders.getKey();
      BaseFunctionHolder holder = holders.getValue();
      totalFuncs++;
      isFunctionSupported(name, holder, fns);

    }
    System.out.println("Total : " + totalFuncs + " unSupported : " + unSupportedFn);
  }

  private boolean isFunctionSupported(String name, BaseFunctionHolder holder, Set<String> fns) throws
    GandivaException {
    String fnToSearch = FunctionCallFactory.replaceOpWithFuncName(name) + "##";
    for (CompleteType arg : holder.getParamTypes()) {
      fnToSearch = fnToSearch + "##" + arg.getType();
    }

    if (!fns.contains(fnToSearch)) {
      unSupportedFn++;
      System.out.println(("function signature not supported in gandiva : " +  fnToSearch));
      return false;
    } else {
      System.out.println(("function signature supported in gandiva : " +  fnToSearch));
    }
    return true;
  }

  private FunctionCall getAddFn() {
    List<LogicalExpression> args = Lists.newArrayList(ValueExpressions.getInt(1), ValueExpressions
      .getInt(2));
    return new FunctionCall("add", args);
  }
}
