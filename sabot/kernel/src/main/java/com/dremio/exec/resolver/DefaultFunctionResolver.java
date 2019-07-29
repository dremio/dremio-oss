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

package com.dremio.exec.resolver;

import java.util.LinkedList;
import java.util.List;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionCall;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.expr.fn.AbstractFunctionHolder;
import com.dremio.exec.util.AssertionUtil;
import com.google.common.collect.Lists;

public class DefaultFunctionResolver implements FunctionResolver {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultFunctionResolver.class);

  @Override
  public AbstractFunctionHolder getBestMatch(List<AbstractFunctionHolder> methods, FunctionCall call) {

    int bestcost = Integer.MAX_VALUE;
    int currcost = Integer.MAX_VALUE;
    AbstractFunctionHolder bestmatch = null;
    final List<AbstractFunctionHolder> bestMatchAlternatives = new LinkedList<>();

    for (AbstractFunctionHolder h : methods) {
      final List<CompleteType> argumentTypes = Lists.newArrayList();
      for (LogicalExpression expression : call.args) {
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

    if (bestcost < 0) {
      //did not find a matched func implementation, either w/ or w/o implicit casts
      //TODO: raise exception here?
      return null;
    } else {
      if (AssertionUtil.isAssertionsEnabled() && bestMatchAlternatives.size() > 0) {
        /*
         * There are other alternatives to the best match function which could have been selected
         * Log the possible functions and the chose implementation and raise an exception
         */
        logger.warn("Chosen function impl: " + bestmatch.toString());

        // printing the possible matches
        logger.warn("Printing all the possible functions that could have matched: ");
        for (AbstractFunctionHolder holder: bestMatchAlternatives) {
          logger.warn(holder.toString());
        }

        // TODO Figure out if this is needed
//        throw new AssertionError("Multiple functions with best cost found");
      }
      return bestmatch;
    }
  }

}
