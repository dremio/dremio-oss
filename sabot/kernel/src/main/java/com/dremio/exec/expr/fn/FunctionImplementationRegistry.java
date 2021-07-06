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
package com.dremio.exec.expr.fn;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.resolver.FunctionResolver;
import com.dremio.exec.resolver.FunctionResolverFactory;
import com.dremio.options.OptionManager;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;

/**
 * This class offers the registry for functions. Notably, in addition to Dremio its functions
 * (in {@link FunctionRegistry}), other PluggableFunctionRegistry (e.g., {com.dremio.exec.expr.fn.HiveFunctionRegistry})
 * is also registered in this class
 */
public class FunctionImplementationRegistry implements FunctionLookupContext {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionImplementationRegistry.class);

  protected List<PrimaryFunctionRegistry> primaryFunctionRegistries = Lists.newArrayList();
  protected FunctionRegistry functionRegistry;
  private final List<PluggableFunctionRegistry> pluggableFuncRegistries;
  private OptionManager optionManager = null;
  private static final Set<String> aggrFunctionNames = Sets.newHashSet("sum", "$sum0", "min",
    "max", "hll");
  protected boolean isDecimalV2Enabled;

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan){
    Stopwatch w = Stopwatch.createStarted();

    logger.debug("Generating function registry.");
    functionRegistry = new FunctionRegistry(classpathScan);
    initializePrimaryRegistries();
    Set<Class<? extends PluggableFunctionRegistry>> registryClasses =
        classpathScan.getImplementations(PluggableFunctionRegistry.class);

    // Create a small Guice module
    final Injector injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        bind(SabotConfig.class).toInstance(config);
        bind(ScanResult.class).toInstance(classpathScan);
      }
    });

    final ImmutableList.Builder<PluggableFunctionRegistry> registries = ImmutableList.builder();
    for (Class<? extends PluggableFunctionRegistry> clazz : registryClasses) {
      try {
        PluggableFunctionRegistry registry = injector.getInstance(clazz);
        registries.add(registry);
      } catch (ProvisionException | ConfigurationException e) {
        logger.warn("Unable to instantiate PluggableFunctionRegistry class '{}'. Skipping it.", clazz, e);
      }
    }

    pluggableFuncRegistries = registries.build();

    logger.info("Function registry loaded. Functions loaded in {} ms.", w.elapsed(TimeUnit
      .MILLISECONDS));
  }

  protected void initializePrimaryRegistries() {
    // this is the registry used, when decimal v2 is turned off.
    isDecimalV2Enabled = false;
    // order is important, first lookup java functions and then gandiva functions
    // if gandiva is preferred code generator, the function would be replaced later.
    primaryFunctionRegistries.add(functionRegistry);
    primaryFunctionRegistries.add(new GandivaFunctionRegistry(isDecimalV2Enabled));
  }

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan, OptionManager optionManager) {
    this(config, classpathScan);
    this.optionManager = optionManager;
  }

  public ArrayListMultimap<String, AbstractFunctionHolder> getRegisteredFunctions() {
    return functionRegistry.getRegisteredFunctions();
  }

  /**
   * Register functions in given operator table.
   * @param operatorTable
   */
  public void register(OperatorTable operatorTable) {
    // Register Dremio functions first and move to pluggable function registries.
    for (PrimaryFunctionRegistry registry : primaryFunctionRegistries) {
      registry.register(operatorTable, isDecimalV2Enabled);
    }

    for(PluggableFunctionRegistry registry : pluggableFuncRegistries) {
      registry.register(operatorTable, isDecimalV2Enabled);
    }
  }

  /**
   * Using the given <code>functionResolver</code> find Dremio function implementation for given
   * <code>functionCall</code>
   *
   * @param functionCall
   * @param allowGandivaFunctions
   * @return
   */
  @Override
  public AbstractFunctionHolder findExactFunction(FunctionCall functionCall, boolean
    allowGandivaFunctions) {
    FunctionResolver functionResolver = FunctionResolverFactory.getExactResolver(functionCall);
    return getMatchingFunctionHolder(functionCall, functionResolver, allowGandivaFunctions);
  }

  @Override
  public AbstractFunctionHolder findFunctionWithCast(FunctionCall functionCall, boolean
    allowGandivaFunctions) {
    FunctionResolver functionResolver = FunctionResolverFactory.getResolver(functionCall);
    return getMatchingFunctionHolder(functionCall, functionResolver, allowGandivaFunctions);
  }

  private AbstractFunctionHolder getMatchingFunctionHolder(FunctionCall functionCall,
                                                           FunctionResolver functionResolver,
                                                           boolean allowGandivaFunctions) {
    FunctionCall functionCallToResolve = getDecimalV2NamesIfEnabled(functionCall);
    List<AbstractFunctionHolder> primaryFunctions = Lists.newArrayList();
    for (PrimaryFunctionRegistry registry : primaryFunctionRegistries) {
      if (registry instanceof GandivaFunctionRegistry && !allowGandivaFunctions) {
        continue;
      }
      List<AbstractFunctionHolder> methods = registry.getMethods(functionCallToResolve.getName());
      primaryFunctions.addAll(methods);
    }
    return functionResolver.getBestMatch(primaryFunctions, functionCallToResolve);
  }

  private FunctionCall getDecimalV2NamesIfEnabled(FunctionCall functionCall) {
    // If decimal v2 is on, switch to function definitions that return decimal vectors
    // for decimal accumulation.
    FunctionCall functionCallToResolve = functionCall;
    if (isDecimalV2Enabled) {
      if (aggrFunctionNames.contains(functionCall.getName().toLowerCase())) {
        LogicalExpression aggrColumn = functionCall.args.get(0);
        if (aggrColumn.getCompleteType().getType().getTypeID() == ArrowType.ArrowTypeID.Decimal) {
          functionCallToResolve = new FunctionCall(functionCall.getName() +
            "_v2", functionCall.args);
        }
      }
    }
    return functionCallToResolve;
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

  @Override
  public OptionManager getOptionManager() {
    return optionManager;
  }

  // Method to find if the output type of a dremio function if of complex type
  public boolean isFunctionComplexOutput(String name) {
    List<AbstractFunctionHolder> methods = functionRegistry.getMethods(name);
    for (AbstractFunctionHolder holder : methods) {
      if (((BaseFunctionHolder)holder).getReturnValue().isComplexWriter()) {
        return true;
      }
    }
    return false;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  @Override
  public boolean isDecimalV2Enabled() {
    return isDecimalV2Enabled;
  }
}
