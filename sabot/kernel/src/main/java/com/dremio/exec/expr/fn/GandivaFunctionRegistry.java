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

import static com.dremio.exec.ExecConstants.DISABLED_GANDIVA_FUNCTIONS;
import static com.dremio.exec.ExecConstants.DISABLED_GANDIVA_FUNCTIONS_ARM;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.planner.sql.Checker;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GandivaFunctionRegistry implements PrimaryFunctionRegistry, OptionChangeListener {

  static final Logger logger = LoggerFactory.getLogger(GandivaFunctionRegistry.class);

  private final Map<String, List<AbstractFunctionHolder>> supportedFunctions =
      CaseInsensitiveMap.newHashMap();
  private final AtomicReference<Set<String>> disabledFunctionsRef;
  private final AtomicReference<Set<String>> disabledArmFunctionsRef;
  private final OptionManager optionManager;

  private volatile boolean listenerAdded;

  public GandivaFunctionRegistry(boolean isDecimalV2Enabled, OptionManager optionManager) {
    this.listenerAdded = (optionManager == null);
    this.optionManager = optionManager;
    try {
      Set<FunctionSignature> supportedFunctions =
          isDecimalV2Enabled
              ? GandivaRegistryWrapper.getInstance().getSupportedFunctionsIncludingDecimal()
              : GandivaRegistryWrapper.getInstance().getSupportedFunctionsExcludingDecimal();
      for (FunctionSignature signature : supportedFunctions) {
        List<AbstractFunctionHolder> signaturesForName =
            this.supportedFunctions.getOrDefault(signature.getName(), Lists.newArrayList());

        // Build the return type.
        final String dataFieldName = "$data$";
        CompleteType retType = null;
        if (!(signature.getReturnListType() instanceof ArrowType.Null)) {
          List<Field> children = new ArrayList<Field>();
          children.add(
              new Field(
                  dataFieldName, new FieldType(true, signature.getReturnListType(), null), null));
          retType = new CompleteType(signature.getReturnType(), children);
        } else {
          retType = new CompleteType(signature.getReturnType());
        }

        // Build the argument types.
        CompleteType[] args = new CompleteType[signature.getParamTypes().size()];

        List<List<ArrowType>> paramTypes = signature.getParamTypes();
        int argIndex = 0;
        for (List<ArrowType> param : paramTypes) {
          if (param.size() == 1) {
            args[argIndex++] = new CompleteType(param.get(0));
          } else if (param.size() == 2 && param.get(0) instanceof ArrowType.List) {
            List<Field> children = new ArrayList<Field>();
            children.add(new Field(dataFieldName, new FieldType(true, param.get(1), null), null));
            args[argIndex++] = new CompleteType(param.get(0), children);
          }
        }
        AbstractFunctionHolder holder =
            new GandivaFunctionHolder(args, retType, signature.getName());
        signaturesForName.add(holder);
        this.supportedFunctions.put(signature.getName(), signaturesForName);
      }
    } catch (GandivaException | UnsatisfiedLinkError e) {
      logger.warn("Unable to instantiate Gandiva. Skipping it.");
    }
    this.disabledFunctionsRef = new AtomicReference<>(Collections.emptySet());
    this.disabledArmFunctionsRef = new AtomicReference<>(Collections.emptySet());
  }

  public static Set<String> toLowerCaseSet(String str) {
    if (str == null) {
      // some tests do not initialize option strings to default values.
      return Collections.emptySet();
    }
    final String cleaned = str.trim().toLowerCase();
    return cleaned.isEmpty()
        ? Collections.emptySet()
        : Arrays.stream(cleaned.split(";")).collect(Collectors.toSet());
  }

  @Override
  public List<SqlOperator> listOperators(boolean isDecimalV2Enabled) {
    if (!listenerAdded) {
      addListener();
    }

    List<SqlOperator> operators = new ArrayList<>();
    final Set<String> disabledFunctions = getDisabledFunctions();
    for (Map.Entry<String, List<AbstractFunctionHolder>> entry : supportedFunctions.entrySet()) {
      final String name = entry.getKey();
      if (disabledFunctions.contains(name)) {
        continue;
      }
      int min = Integer.MAX_VALUE, max = 0;
      for (AbstractFunctionHolder holder : entry.getValue()) {
        if (holder.getParamCount() < min) {
          min = holder.getParamCount();
        }
        if (holder.getParamCount() > max) {
          max = holder.getParamCount();
        }
      }

      SqlOperator operator =
          SqlFunctionImpl.create(
              name,
              TypeInferenceUtils.getSqlReturnTypeInference(supportedFunctions.get(name)),
              Checker.between(min, max),
              SqlFunctionImpl.Source.GANDIVA);
      operators.add(operator);
    }

    return operators;
  }

  @Override
  public List<AbstractFunctionHolder> lookupMethods(String name) {
    if (!listenerAdded) {
      addListener();
    }
    final String lcName = name.toLowerCase();
    final Set<String> disabledFunctions = getDisabledFunctions();
    return disabledFunctions.contains(lcName)
        ? Collections.emptyList()
        : supportedFunctions.getOrDefault(lcName, Collections.emptyList());
  }

  @Override
  public synchronized void onChange() {
    if (optionManager == null) {
      return;
    }
    if (!disabledFunctionsRef
        .get()
        .equals(toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)))) {
      this.disabledFunctionsRef.set(
          toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)));
    }
    if (!disabledArmFunctionsRef
        .get()
        .equals(toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS_ARM)))) {
      this.disabledArmFunctionsRef.set(
          toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS_ARM)));
    }
  }

  private void addListener() {
    this.disabledFunctionsRef.set(
        toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)));
    this.disabledArmFunctionsRef.set(
        toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS_ARM)));
    optionManager.addOptionChangeListener(this);
    listenerAdded = true;
  }

  private Set<String> getDisabledFunctions() {
    Set<String> functions = new HashSet<>(disabledFunctionsRef.get());
    if ("aarch64".equals(System.getProperty("os.arch"))) {
      functions.addAll(disabledArmFunctionsRef.get());
    }
    return functions;
  }
}
