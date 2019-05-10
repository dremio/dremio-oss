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

import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.options.OptionManager;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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
  private static final Set<String> aggrFunctionNames = Sets.newHashSet("sum", "$sum0", "min",
    "max", "hll");
  protected boolean isDecimalV2Enabled;

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan){
    Stopwatch w = Stopwatch.createStarted();

    logger.debug("Generating function registry.");
    functionRegistry = new FunctionRegistry(classpathScan);

    Set<Class<? extends PluggableFunctionRegistry>> registryClasses =
        classpathScan.getImplementations(PluggableFunctionRegistry.class);

    for (Class<? extends PluggableFunctionRegistry> clazz : registryClasses) {
      // We want to add Gandiva after all the other repositories so as to not
      // accidentally override a function from another repository.
      // If a function is supported in Gandiva it can take precedence later using
      // the preferred code generator.
      if (clazz.getName().equals(GandivaFunctionRegistry.class.getName())) {
        continue;
      }
      instantiateAndAddRepo(config, clazz);
    }
    // All others are added should be safe to add Gandiva now.
    instantiateAndAddRepo(config, GandivaFunctionRegistry.class);
    // this is the registry used, when decimal v2 is turned off.
    isDecimalV2Enabled = false;
    logger.info("Function registry loaded.  {} functions loaded in {} ms.", functionRegistry.size(), w.elapsed(TimeUnit.MILLISECONDS));
  }

  private void instantiateAndAddRepo(SabotConfig config, Class<? extends PluggableFunctionRegistry> clazz) {
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
      } catch(InstantiationException | IllegalAccessException | IllegalArgumentException |
          InvocationTargetException e) {
        logger.warn("Unable to instantiate PluggableFunctionRegistry class '{}'. Skipping it.", clazz, e);
      }

      break;
    }
  }

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan, OptionManager optionManager) {
    this(config, classpathScan);
    this.optionManager = optionManager;
  }

  public ArrayListMultimap<String, BaseFunctionHolder> getRegisteredFunctions() {
    return functionRegistry.getRegisteredFunctions();
  }

  /**
   * Register functions in given operator table.
   * @param operatorTable
   */
  public void register(OperatorTable operatorTable) {
    // Register Dremio functions first and move to pluggable function registries.
    functionRegistry.register(operatorTable, isDecimalV2Enabled);

    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      registry.register(operatorTable, isDecimalV2Enabled);
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
    FunctionCall functionCallToResolve = functionCall;
    // If decimal v2 is on, switch to function definitions that return decimal vectors
    // for decimal accumulation.
    if (isDecimalV2Enabled) {
      if (aggrFunctionNames.contains(functionCall.getName().toLowerCase())) {
        LogicalExpression aggrColumn = functionCall.args.get(0);
        if (aggrColumn.getCompleteType().getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
          functionCallToResolve = new FunctionCall(functionCall.getName() +
            "_v2", functionCall.args);
        }
      }
    }
    return functionResolver.getBestMatch(functionRegistry.getMethods(functionCallToResolve.getName()),
      functionCallToResolve, isDecimalV2Enabled);
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
      AbstractFunctionHolder h = registry.getFunction(functionCall, isDecimalV2Enabled);
      if (h != null) {
        return h;
      }
    }

    return null;
  }

  @Override
  public OptionManager getOptionManager() {
    return optionManager;
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

  public boolean isDecimalV2Enabled() {
    return isDecimalV2Enabled;
  }
}
