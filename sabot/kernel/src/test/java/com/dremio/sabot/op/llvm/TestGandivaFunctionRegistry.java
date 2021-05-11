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
package com.dremio.sabot.op.llvm;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.FunctionCallFactory;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.ExecTest;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionRegistry;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.exec.resolver.FunctionResolverFactory;
import com.dremio.options.OptionManager;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestGandivaFunctionRegistry extends ExecTest {

  private static int totalFuncs = 0, unSupportedFn = 0;

  private static final OptionManager optionManager = Mockito.mock(OptionManager.class);
  /*
   * Test that function lookups on the gandiva repository works.
   */
  @Test
  public void testGandivaRegistry() throws Exception {
    GandivaFunctionRegistry fnRegistry = new GandivaFunctionRegistry(true);

    FunctionCall fnCall = getGandivaOnlyFn();
    FunctionResolver resolver = FunctionResolverFactory.getResolver(fnCall);
    GandivaFunctionHolder holder = (GandivaFunctionHolder)resolver.getBestMatch(fnRegistry
      .getMethods(fnCall.getName()), fnCall);
    Assert.assertNotNull(holder);
    CompleteType expectedReturnType = CompleteType.fromMinorType(TypeProtos.MinorType.BIT);
    Assert.assertEquals(expectedReturnType, holder.getReturnType(fnCall.args));
    Assert.assertNotNull(holder.getExpr(fnCall.getName(), fnCall.args));
  }

  /*
   * Test that dremio repository is integrated with Gandiva as a primary repository
   */
  @Test
  public void testFunctionImplementationRegistry() {
    FunctionImplementationRegistry fnRegistry = DECIMAL_FUNCTIONS();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.findExactFunction(getDecimalSameFn()
      , true);
    Assert.assertNotNull(holder);

    // test that gandiva functions are not used where they are not intended.
    holder = (GandivaFunctionHolder)fnRegistry.findExactFunction(getDecimalSameFn()
      , false);
    Assert.assertNull(holder);
  }

  /*
   * Test that lookup on non-decimal repository does not return decimal functions.
   */
  @Test
  public void testNonDecimalFunctionRegistry() {
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    GandivaFunctionHolder holder = (GandivaFunctionHolder)fnRegistry.findExactFunction(getDecimalSameFn()
      , true);
    Assert.assertNull(holder);
  }

  /*
   * Test that lookup on non-decimal repository does not return decimal functions.
   */
  @Test
  public void testNonDecimalGandivaRegistry() {
    GandivaFunctionRegistry fnRegistry = new GandivaFunctionRegistry(false);
    FunctionCall fnCall = getDecimalAddFn();
    FunctionResolver resolver = FunctionResolverFactory.getExactResolver(fnCall);
    GandivaFunctionHolder holder = (GandivaFunctionHolder)resolver.getBestMatch(fnRegistry
      .getMethods(fnCall.getName()), fnCall);
    Assert.assertNull(holder);
  }

  @Test
  public void getAllRegisteredFunctionsAndGenerateYAML() throws IOException {
    Map<String, Map<String, Object>> functionsToSave = new HashMap<>();

    // Retrieve the function registry (FOR TEST PURPOSE)
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    ArrayListMultimap<String, AbstractFunctionHolder> functions = fnRegistry.getRegisteredFunctions();

    // ObjectMapper is instantiated to map the YAML file
    ObjectMapper om = new ObjectMapper(new YAMLFactory());

    Map<String, Object> stringObjectMap;
    // Iterate over each registered function to extract the available information
    for (Map.Entry<String, AbstractFunctionHolder> holders : functions.entries()) {
      String functionName = holders.getKey().toLowerCase();
      BaseFunctionHolder value = (BaseFunctionHolder) holders.getValue();

      stringObjectMap = functionsToSave.get(functionName);
      if (stringObjectMap == null) {
        stringObjectMap = new HashMap<>();
      }
      List<Map<String, Object>> signaturesList = (List<Map<String, Object>>) stringObjectMap.get("signatures");
      if (signaturesList == null) {
        signaturesList = new ArrayList<>();
      }

      // Define signatures values
      Map<String, Object> signaturesValues = new HashMap<>();
      List<Map<String, Object>> parametersList = new ArrayList<>();
      int count = 1;
      for (BaseFunctionHolder.ValueReference parameter : value.getParameters()) {
        int finalCount = count;
        parametersList.add(
          new HashMap<String, Object>() {
            {
              put("ordinalPosition", finalCount);
              put("parameterName", parameter.getName());
              put("parameterType", parameter.getType().toString());
              try {
                put("parameterFormat", (parameter.getType().toMinorType()));
              } catch (Exception exception) {}
            }
          }
        );
        count++;
      }
      signaturesValues.put("parameterList", parametersList);
      signaturesValues.put("returnType", value.getReturnValue().getType().toString());
      signaturesList.add(signaturesValues);
      stringObjectMap.put("signatures", signaturesList);

      // TODO: use the correct path according to review
      functionsToSave.put(functionName, stringObjectMap);
//      break;
    }
    om.writeValue(new File("/home/jpedroantunes/dremio/functions.yaml"), functionsToSave);
  }

  @Test
  public void getUnSupportedFunctions() throws GandivaException {
    FunctionImplementationRegistry fnRegistry = FUNCTIONS();
    ArrayListMultimap<String, AbstractFunctionHolder> functions = fnRegistry.getRegisteredFunctions();
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
    for (Map.Entry<String, AbstractFunctionHolder> holders : functions.entries()) {
      String name = holders.getKey();
      AbstractFunctionHolder holder = holders.getValue();
      totalFuncs++;
      isFunctionSupported(name, (BaseFunctionHolder)holder, fns);

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

  private FunctionCall getGandivaOnlyFn() {
    List<LogicalExpression> args = Lists.newArrayList(ValueExpressions.getChar("test"),
      ValueExpressions.getChar("tes"));
    return new FunctionCall("starts_with", args);
  }

  private FunctionCall getDecimalAddFn() {
      List<LogicalExpression> args = Lists.newArrayList(
        ValueExpressions.getDecimal(BigDecimal.valueOf(1), 1, 0),
        ValueExpressions.getDecimal(BigDecimal.valueOf(2), 1, 0));
      return new FunctionCall("add", args);
  }

  private FunctionCall getDecimalSameFn() {
    List<LogicalExpression> args = Lists.newArrayList(
      ValueExpressions.getDecimal(BigDecimal.valueOf(1), 1, 0),
      ValueExpressions.getDecimal(BigDecimal.valueOf(2), 1, 0));
    return new FunctionCall("same", args);
  }

}
