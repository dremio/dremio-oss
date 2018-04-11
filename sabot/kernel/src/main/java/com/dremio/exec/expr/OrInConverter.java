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
package com.dremio.exec.expr;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.LogicalExpression;
import com.google.common.collect.ArrayListMultimap;

/**
 * Finds collections of OR conditions that are the same expression. Will replace
 * with a special IN function that can be executed in an optimal manner.
 */
public class OrInConverter {

  public static List<LogicalExpression> optimizeMultiOrs(List<LogicalExpression> expressions, Set<LogicalExpression> constants, int minConversionSize) {

    // collect all subtrees that aren't possible.
    final List<LogicalExpression> notPossibles = new ArrayList<>();

    // collect or conditions that could possible be used for replacement.
    ArrayListMultimap<PossibleKey, Possible> possibles = ArrayListMultimap.create();

    for(LogicalExpression expr : expressions) {
      Possible possible = getPossible(expr, constants);
      if(possible == null) {
        notPossibles.add(expr);
      } else {
        possibles.put(possible.key, possible);
      }
    }

    final List<LogicalExpression> finalConditions = new ArrayList<>();
    for(PossibleKey key : possibles.keySet()) {
      List<Possible> inItems = possibles.get(key);
      if(inItems.size() < minConversionSize) {
        for(Possible possible : inItems) {
          notPossibles.add(possible.original);
        }
        continue;
      }

      // create the arguments for a specialized in operator.
      List<LogicalExpression> arguments = new ArrayList<>();

      for(Possible p : inItems) {
        arguments.add(p.constant);
      }

      finalConditions.add(new InExpression(key.key, arguments));
    }

    if(finalConditions.isEmpty()) {
      return expressions;
    }

    finalConditions.addAll(notPossibles);

    return finalConditions;
  }

  /**
   * Determine if the provided expression is an expression of RexInputRef == RexLiteral or RexLiteral == RexInputRef.
   */
  private static Possible getPossible(LogicalExpression expr, Set<LogicalExpression> constants) {
    if( !(expr instanceof FunctionHolderExpression) ) {
      return null;
    }

    FunctionHolderExpression func = (FunctionHolderExpression) expr;

    if(!"equal".equals(func.getName())) {
      return null;
    }

    // no point to optimize constant expressions.
    if(constants.contains(expr)) {
      return null;
    }

    List<LogicalExpression> args = func.args;
    if(args.size() != 2) {
      return null;
    }

    LogicalExpression arg0 = args.get(0);
    LogicalExpression arg1 = args.get(1);

    if(!arg0.getCompleteType().equals(arg1.getCompleteType())) {
      return null;
    }

    final CompleteType type = arg0.getCompleteType();

    if( !(
        CompleteType.BIGINT.equals(type) ||
        CompleteType.INT.equals(type) ||
        CompleteType.DATE.equals(type) ||
        CompleteType.TIME.equals(type) ||
        CompleteType.TIMESTAMP.equals(type) ||
        CompleteType.VARCHAR.equals(type) ||
        CompleteType.VARBINARY.equals(type)
        )
        ){
     return null;
    }


    if(constants.contains(arg0)) {
      return Possible.getPossible(arg1, arg0, func);
    }else if(constants.contains(arg1)) {
      return Possible.getPossible(arg0, arg1, func);
    }else {
      return null;
    }
  }

  private static class PossibleKey {
    private final LogicalExpression key;

    public PossibleKey(LogicalExpression key) {
      super();
      this.key = key;
    }

    @Override
    public int hashCode() {
      return key.toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      PossibleKey other = (PossibleKey) obj;
      return key.accept(new EqualityVisitor(), other.key);
    }
  }

  private static class Possible {
    private final PossibleKey key;
    private final LogicalExpression constant;
    private final LogicalExpression original;

    private Possible(LogicalExpression key, LogicalExpression constant, LogicalExpression original) {
      super();
      this.key = new PossibleKey(key);
      this.constant = constant;
      this.original = original;
    }

    static Possible getPossible(LogicalExpression input, LogicalExpression literal, LogicalExpression original) {
      if(input.getCompleteType().equals(literal.getCompleteType())) {
        return new Possible(input, literal, original);
      }

      return null;
    }
  }

}
