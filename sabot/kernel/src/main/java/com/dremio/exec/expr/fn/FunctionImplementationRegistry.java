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
import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.dremio.exec.store.sys.functions.FunctionParameterInfo;
import com.dremio.exec.store.sys.functions.SysTableFunctionsInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.commons.lang3.StringUtils;

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
  private SabotConfig sabotConfig;

  public FunctionImplementationRegistry(SabotConfig config, ScanResult classpathScan) {
    Stopwatch w = Stopwatch.createStarted();
    this.sabotConfig = config;
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
//    this.generateYAMLWithRegisteredFunctions();
  }

  public ArrayListMultimap<String, AbstractFunctionHolder> getRegisteredFunctions() {
    return functionRegistry.getRegisteredFunctions();
  }

  /**
   * Register functions in given operator table.
   *
   * @param operatorTable
   */
  public void register(OperatorTable operatorTable) {
    // Register Dremio functions first and move to pluggable function registries.
    for (PrimaryFunctionRegistry registry : primaryFunctionRegistries) {
      registry.register(operatorTable, isDecimalV2Enabled);
    }

    for (PluggableFunctionRegistry registry : pluggableFuncRegistries) {
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
   * <p>
   * Note: Order of searching is same as order of {@link com.dremio.exec.expr.fn.PluggableFunctionRegistry}
   * implementations found on classpath.
   *
   * @param functionCall
   * @return
   */
  @Override
  public AbstractFunctionHolder findNonFunction(FunctionCall functionCall) {
    for (PluggableFunctionRegistry registry : pluggableFuncRegistries) {
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
      if (((BaseFunctionHolder) holder).getReturnValue().isComplexWriter()) {
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

  public Map<String, Map<String, Object>> generateMapWithRegisteredFunctions() {
    // Retrieve the registered functions on the function registry
    ArrayListMultimap<String, AbstractFunctionHolder> functions = this.getRegisteredFunctions();

    Map<String, Map<String, Object>> functionsToSave = new HashMap<>();
    Map<String, Object> stringObjectMap;
    // Iterate over each registered function to extract the available information
    for (Map.Entry<String, AbstractFunctionHolder> holders : functions.entries()) {
      String functionName = holders.getKey().toLowerCase();
      BaseFunctionHolder value = (BaseFunctionHolder) holders.getValue();

      stringObjectMap = functionsToSave.get(functionName);
      if (stringObjectMap == null) {
        stringObjectMap = new HashMap<>();
        stringObjectMap.put("functionName", functionName.toUpperCase());
        stringObjectMap.put("dremioVersions", null);
        stringObjectMap.put("description", null);
        stringObjectMap.put("extendedDescription", null);
        stringObjectMap.put("useCaseExamples", null);
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
              } catch (Exception ignored) {
              }
            }
          }
        );
        count++;
      }
      signaturesValues.put("parameterList", parametersList);
      signaturesValues.put("returnType", value.getReturnValue().getType().toString());
      signaturesList.add(signaturesValues);
      stringObjectMap.put("signatures", signaturesList);

      functionsToSave.put(functionName, stringObjectMap);
    }
    return functionsToSave;
  }

//  public void generateYAMLWithRegisteredFunctions() {
//    logger.info("Starting to generate yaml with registered functions...");
//    // ObjectMapper is instantiated to map the YAML file
//    ObjectMapper om = new ObjectMapper(new YAMLFactory());
//    Map<String, Map<String, Object>> functionsToSave = this.generateMapWithRegisteredFunctions();
//
//    try {
//      File fileToSave = new File(System.getProperty("user.dir") + "/target/registered_functions.yaml");
//      om.writeValue(fileToSave, functionsToSave);
//      logger.info("Finished to generate yaml with registered functions at {}.", fileToSave.getAbsolutePath());
//    } catch (Exception exception) {
//      logger.warn("Failed generating YAML function files: " + exception.getMessage());
//    }
//  }

  public List<SysTableFunctionsInfo> generateListWithCalciteFunctions() {
    OperatorTable operatorTable = new OperatorTable(this);
    List<SysTableFunctionsInfo> functionsInfoList = new ArrayList<>();

    for (SqlOperator operator : operatorTable.getOperatorList()) {
      String opName = operator.getName();
      // Only SQL's functions are uppercased
      opName = opName.toLowerCase(Locale.ROOT);
      logger.info("opName: {}", opName);
      //  SqlReturnTypeInference returnType = operator.getReturnTypeInference().inferReturnType();
      String returnType = "";
      List<FunctionParameterInfo> parameterInfoList = new ArrayList<>();
      SqlSyntax syntax = operator.getSyntax();
      if (syntax.toString() == "FUNCTION") {
        String signaturesString = "";
        try {
          // Get the function's signature, it will come like FUNCTION_NAME(<PARAM1_TYPE> <PARAM2_TYPE>)
          signaturesString = operator.getAllowedSignatures();
        } catch (Exception e) {
          logger.warn("Failed to read Calcite {} function's allowed signatures, with exception {}. ", opName, e);
        }
        // Get a list of allowed signatures that come in a single string
        List<String> signatures = Arrays.asList(signaturesString.split("\\r?\\n"));
        for (String signature : signatures) {
          // Get only the param's type for each signature
          String[] ps = StringUtils.substringsBetween(signature, "<", ">");
          if (ps != null) {
            List<String> params = Arrays.asList(ps);
            for (int i = 0; i < params.size() ; i++) {
              // Based on the doc, operators have similar, straightforward strategies,
              // such as to take the type of the first operand to be its return type
              if  (i == 0) {
                returnType = params.get(i).toLowerCase(Locale.ROOT);
              }
              parameterInfoList.add(new FunctionParameterInfo("param-" + i,
                params.get(i).toLowerCase(Locale.ROOT),
                false));
            }
          }
          if (returnType.equals("") || returnType.isEmpty()) {
            returnType = "inferred at runtime";
          }
          SysTableFunctionsInfo toAdd = new SysTableFunctionsInfo(opName, returnType, parameterInfoList.toString());

          // Avoid possible duplicates
          if (!functionsInfoList.contains(toAdd)) {
            functionsInfoList.add(toAdd);
          }
        }
      }
    }
    return functionsInfoList;
  }
}
