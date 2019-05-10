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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.resolver.TypeCastRules;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class GandivaFunctionRegistry implements PluggableFunctionRegistry {

  static final Logger logger = LoggerFactory.getLogger(GandivaFunctionRegistry.class);

  private final Map<String, Set<GandivaFunctionHolder>> supportedFunctions = Maps.newHashMap();

  public GandivaFunctionRegistry(SabotConfig config) throws InstantiationException {
    try {
      Set<FunctionSignature> supportedFunctions = ExpressionRegistry.getInstance()
        .getSupportedFunctions();
      for (FunctionSignature signature : supportedFunctions) {
        Set<GandivaFunctionHolder> signaturesForName = this.supportedFunctions.getOrDefault(
          signature.getName(),Sets.newHashSet());

        CompleteType retType = new CompleteType(signature.getReturnType());
        CompleteType[] args = new CompleteType[signature.getParamTypes().size()];
        signature.getParamTypes()
          .stream()
          .map(arrowType -> new CompleteType (arrowType))
          .collect(Collectors.<CompleteType>toList())
          .toArray(args);
        GandivaFunctionHolder holder = new GandivaFunctionHolder(args,retType,signature.getName());
        signaturesForName.add(holder);
        this.supportedFunctions.put(signature.getName(), signaturesForName);
      }
    } catch (GandivaException | UnsatisfiedLinkError e) {
      throw new InstantiationException("Error in creating the gandiva repository."
        + e.getMessage());
    }
  }

  @Override
  public void register(OperatorTable operatorTable, boolean isDecimalV2Enabled) {

    for (String name : supportedFunctions.keySet()) {
      for (GandivaFunctionHolder holder : supportedFunctions.get(name)) {
        SqlOperator operator  = GandivaOperator.getSimpleFunction(name, holder.getParamCount(), new
          PlugginRepositorySqlReturnTypeInference(this, isDecimalV2Enabled));
        operatorTable.add(name, operator);
      }
    }
  }

  @Override
  public AbstractFunctionHolder getFunction(FunctionCall functionCall, boolean isDecimalV2Enabled) {
    int bestcost = Integer.MAX_VALUE;
    int currcost = Integer.MAX_VALUE;
    GandivaFunctionHolder bestmatch = null;
    final List<GandivaFunctionHolder> bestMatchAlternatives = new LinkedList<>();

    Set<GandivaFunctionHolder> methods = supportedFunctions.get(functionCall.getName()) ;

    // maybe a Hive function.
    if (methods == null ){
      return null;
    }

    for (GandivaFunctionHolder h : methods) {
      final List<CompleteType> argumentTypes = Lists.newArrayList();
      for (LogicalExpression expression : functionCall.args) {
        argumentTypes.add(expression.getCompleteType());
      }
      currcost = TypeCastRules.getCost(argumentTypes, h, isDecimalV2Enabled);

      // if cost is lower than 0, func implementation is not matched, either w/ or w/o implicit casts
      if (currcost  < 0 ) {
        continue;
      }

      if (currcost < bestcost) {
        bestcost = currcost;
        bestmatch = h;
        bestMatchAlternatives.clear();
      } else if (currcost == bestcost) {
        // keep log of different function implementations that have the same best cost
        bestMatchAlternatives.add(h);
      }
    }
    if (bestMatchAlternatives.size() > 1) {
      logger.warn("more than one alternative while converting call " + functionCall.toString()
                  + "alternatives " + bestMatchAlternatives);
    }
    return bestmatch;
  }
}
