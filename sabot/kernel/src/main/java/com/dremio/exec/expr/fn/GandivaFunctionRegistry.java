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

import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.resolver.TypeCastRules;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.arrow.gandiva.evaluator.ExpressionRegistry;
import org.apache.arrow.gandiva.evaluator.FunctionSignature;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GandivaFunctionRegistry {

  static final Logger logger = LoggerFactory.getLogger(BaseFunctionHolder.class);

  private final Map<String, Set<GandivaFunctionHolder>> supportedFunctions = Maps.newHashMap();

  public GandivaFunctionRegistry(SabotConfig config) throws GandivaException {
    for (FunctionSignature signature : ExpressionRegistry.getInstance().getSupportedFunctions()) {
      Set<GandivaFunctionHolder> signaturesForName = supportedFunctions.getOrDefault(
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
      supportedFunctions.put(signature.getName(), signaturesForName);
    }
  }
  public void register(OperatorTable operatorTable) {
    //TODO : Nothing for now. handled in https://dremio.atlassian.net/browse/DX-12539
  }

  public AbstractFunctionHolder getFunction(FunctionCall functionCall) {
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
      currcost = TypeCastRules.getCost(argumentTypes, h);

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
