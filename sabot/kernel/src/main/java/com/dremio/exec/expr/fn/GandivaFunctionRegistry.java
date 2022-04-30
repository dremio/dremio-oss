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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.google.common.collect.Lists;

public class GandivaFunctionRegistry implements PrimaryFunctionRegistry, OptionChangeListener {

  static final Logger logger = LoggerFactory.getLogger(GandivaFunctionRegistry.class);

  private final Map<String, List<AbstractFunctionHolder>> supportedFunctions = CaseInsensitiveMap.newHashMap();
  private final AtomicReference<Set<String>> disabledFunctionsRef;
  private final OptionManager optionManager;

  private volatile boolean listenerAdded;

  public GandivaFunctionRegistry(boolean isDecimalV2Enabled, OptionManager optionManager) {
    this.listenerAdded = (optionManager == null);
    this.optionManager = optionManager;
    try {
      Set<FunctionSignature> supportedFunctions = isDecimalV2Enabled ? GandivaRegistryWrapper
        .getInstance().getSupportedFunctionsIncludingDecimal() : GandivaRegistryWrapper
        .getInstance().getSupportedFunctionsExcludingDecimal();
      for (FunctionSignature signature : supportedFunctions) {
        List<AbstractFunctionHolder> signaturesForName = this.supportedFunctions.getOrDefault(
          signature.getName(), Lists.newArrayList());

        CompleteType retType = new CompleteType(signature.getReturnType());
        CompleteType[] args = new CompleteType[signature.getParamTypes().size()];
        signature.getParamTypes()
          .stream()
          .map(CompleteType::new)
          .collect(Collectors.<CompleteType>toList())
          .toArray(args);
        AbstractFunctionHolder holder = new GandivaFunctionHolder(args, retType, signature.getName());
        signaturesForName.add(holder);
        this.supportedFunctions.put(signature.getName(), signaturesForName);
      }
    } catch (GandivaException | UnsatisfiedLinkError e) {
      logger.warn("Unable to instantiate Gandiva. Skipping it.");
    }
    this.disabledFunctionsRef = new AtomicReference<>(Collections.emptySet());
  }

  public static Set<String> toLowerCaseSet(String str) {
    if (str == null) {
      // some tests do not initialize option strings to default values.
      return Collections.emptySet();
    }
    final String cleaned = str.trim().toLowerCase();
    return cleaned.isEmpty() ? Collections.emptySet() : Arrays.stream(cleaned.split(";")).collect(Collectors.toSet());
  }

  @Override
  public void register(OperatorTable operatorTable, boolean isDecimalV2Enabled) {
    if (!listenerAdded) {
      addListener();
    }
    final Set<String> disabledFunctions = disabledFunctionsRef.get();
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
      SqlOperator operator = GandivaOperator.getSimpleFunction(name, min, max,
        TypeInferenceUtils.getSqlReturnTypeInference(name, supportedFunctions.get(name), isDecimalV2Enabled));
      operatorTable.add(name, operator);
    }
  }

  @Override
  public List<AbstractFunctionHolder> getMethods(String name) {
    if (!listenerAdded) {
      addListener();
    }
    final String lcName = name.toLowerCase();
    final Set<String> disabledFunctions = disabledFunctionsRef.get();
    return disabledFunctions.contains(lcName) ? Collections.emptyList() :
      supportedFunctions.getOrDefault(lcName, Collections.emptyList());
  }

  @Override
  public synchronized void onChange() {
    if (optionManager == null ||
      disabledFunctionsRef.get().equals(toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)))) {
      return;
    }
    // no need to compare if there is a change as option changes
    this.disabledFunctionsRef.set(toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)));
  }

  private void addListener() {
    this.disabledFunctionsRef.set(toLowerCaseSet(optionManager.getOption(DISABLED_GANDIVA_FUNCTIONS)));
    optionManager.addOptionChangeListener(this);
    listenerAdded = true;
  }
}
