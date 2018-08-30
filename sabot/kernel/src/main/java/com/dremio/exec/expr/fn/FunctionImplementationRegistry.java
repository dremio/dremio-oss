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
package com.dremio.exec.expr.fn;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.options.OptionManager;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * This class offers the registry for functions. Notably, in addition to Dremio its functions
 * (in {@link FunctionRegistry}), other PluggableFunctionRegistry (e.g., {@link com.dremio.exec.expr.fn.HiveFunctionRegistry})
 * is also registered in this class
 */
public class FunctionImplementationRegistry implements FunctionLookupContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);

  private FunctionRegistry functionRegistry;
  private List<PluggableFunctionRegistry> pluggableFuncRegistries = Lists.newArrayList();
  private OptionManager optionManager = null;

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan){
    Stopwatch w = Stopwatch.createStarted();

    logger.debug("Generating function registry.");
    functionRegistry = new FunctionRegistry(classpathScan);

    Set<Class<? extends PluggableFunctionRegistry>> registryClasses =
        classpathScan.getImplementations(PluggableFunctionRegistry.class);

    for (Class<? extends PluggableFunctionRegistry> clazz : registryClasses) {
      for (Constructor<?> c : clazz.getConstructors()) {
        Class<?>[] params = c.getParameterTypes();
        if (params.length != 1 || params[0] != SabotConfig.class) {
          logger.warn("Skipping PluggableFunctionRegistry constructor {} for class {} since it doesn't implement a " +
              "[constructor(SabotConfig)]", c, clazz);
          continue;
        }

        try {
          PluggableFunctionRegistry registry = (PluggableFunctionRegistry)c.newInstance(config);
          pluggableFuncRegistries.add(registry);
        } catch(InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          logger.warn("Unable to instantiate PluggableFunctionRegistry class '{}'. Skipping it.", clazz, e);
        }

        break;
      }
    }
    logger.info("Function registry loaded.  {} functions loaded in {} ms.", functionRegistry.size(), w.elapsed(TimeUnit.MILLISECONDS));
  }

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan, OptionManager optionManager) {
    this(config, classpathScan);
    this.optionManager = optionManager;
  }

  /**
   * Register functions in given operator table.
   * @param operatorTable
   */
  public void register(OperatorTable operatorTable) {
    // Register Dremio functions first and move to pluggable function registries.
    functionRegistry.register(operatorTable);

    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      registry.register(operatorTable);
    }
  }

  /**
   * Using the given <code>functionResolver</code> find Dremio function implementation for given
   * <code>functionCall</code>
   *
   * @param functionResolver
   * @param functionCall
   * @return
   */
  @Override
  public BaseFunctionHolder findFunction(FunctionResolver functionResolver, FunctionCall functionCall) {
    return functionResolver.getBestMatch(functionRegistry.getMethods(functionCall.getName()), functionCall);
  }

  /**
   * Find the Dremio function implementation that matches the name, arg types and return type.
   * @param name
   * @param argTypes
   * @param returnType
   * @return
   */
  public BaseFunctionHolder findExactMatchingFunction(String name, List<CompleteType> argTypes, CompleteType returnType) {
    for (BaseFunctionHolder h : functionRegistry.getMethods(name)) {
      if (h.matches(returnType, argTypes)) {
        return h;
      }
    }

    return null;
  }

  /**
   * Find function implementation for given <code>functionCall</code> in non-Dremio function registries such as Hive UDF
   * registry.
   *
   * Note: Order of searching is same as order of {@link com.dremio.exec.expr.fn.PluggableFunctionRegistry}
   * implementations found on classpath.
   *
   * @param functionCall
   * @return
   */
  @Override
  public AbstractFunctionHolder findNonFunction(FunctionCall functionCall) {
    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      AbstractFunctionHolder h = registry.getFunction(functionCall);
      if (h != null) {
        return h;
      }
    }

    return null;
  }

  // Method to find if the output type of a dremio function if of complex type
  public boolean isFunctionComplexOutput(String name) {
    List<BaseFunctionHolder> methods = functionRegistry.getMethods(name);
    for (BaseFunctionHolder holder : methods) {
      if (holder.getReturnValue().isComplexWriter()) {
        return true;
      }
    }
    return false;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }
}
