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
package com.dremio.common.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class FunctionCallFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionCallFactory.class);

  private static Map<String, String> opToFuncTable = new HashMap<>();

  static {
    opToFuncTable.put("+", "add");
    opToFuncTable.put("-", "subtract");
    opToFuncTable.put("/", "divide");
    opToFuncTable.put("*", "multiply");
    opToFuncTable.put("%", "modulo");
    opToFuncTable.put("^", "xor");
    opToFuncTable.put("||", "concatOperator");
    opToFuncTable.put("or", "booleanOr");
    opToFuncTable.put("and", "booleanAnd");
    opToFuncTable.put(">", "greater_than");
    opToFuncTable.put("<", "less_than");
    opToFuncTable.put("==", "equal");
    opToFuncTable.put("=", "equal");
    opToFuncTable.put("!=", "not_equal");
    opToFuncTable.put("<>", "not_equal");
    opToFuncTable.put(">=", "greater_than_or_equal_to");
    opToFuncTable.put("<=", "less_than_or_equal_to");
    opToFuncTable.put("is null", "isnull");
    opToFuncTable.put("is not null", "isnotnull");
    opToFuncTable.put("is true", "istrue");
    opToFuncTable.put("is not true", "isnottrue");
    opToFuncTable.put("is false", "isfalse");
    opToFuncTable.put("is not false", "isnotfalse");
    opToFuncTable.put("similar to", "similar_to");
    opToFuncTable.put("is distinct from", "is_distinct_from");
    opToFuncTable.put("is not distinct from", "is_not_distinct_from");

    opToFuncTable.put("!", "not");
    opToFuncTable.put("u-", "negative");
  }

  public static String replaceOpWithFuncName(String op) {
    return (opToFuncTable.containsKey(op)) ? (opToFuncTable.get(op)) : op;
  }

  public static boolean isBooleanOperator(String funcName) {
    String opName = replaceOpWithFuncName(funcName);
    return "booleanAnd".equals(opName) || "booleanOr".equals(opName);
  }

  /*
   * create a cast function.
   * arguments : type -- targetType
   *             ep   -- input expression position
   *             expr -- input expression
   */
  public static LogicalExpression createCast(com.dremio.common.types.TypeProtos.MajorType type, LogicalExpression expr) {
    return new CastExpression(expr, type);
  }

  public static LogicalExpression createConvert(String function, String conversionType, LogicalExpression expr) {
    return new ConvertExpression(function, conversionType, expr);
  }

  public static LogicalExpression createConvertReplace(String function, ValueExpressions.QuotedString conversionType,
                                                       LogicalExpression inputExpr, ValueExpressions.QuotedString replacement) {
    if (conversionType.value.equalsIgnoreCase("UTF8")) {
      return new FunctionCall("convert_replaceUTF8", ImmutableList.of(inputExpr, replacement));
    } else {
      return new FunctionCall(function, ImmutableList.of(inputExpr, conversionType, replacement));
    }
  }


  public static LogicalExpression createExpression(String functionName, List<LogicalExpression> args){
    String name = replaceOpWithFuncName(functionName);
    if (isBooleanOperator(name)) {
      return new BooleanOperator(name, args);
    } else {
      return new FunctionCall(name, args);
    }
  }

  public static LogicalExpression createExpression(String functionName, LogicalExpression... e){
    return createExpression(functionName, Lists.newArrayList(e));
  }

  public static LogicalExpression createBooleanOperator(String functionName, List<LogicalExpression> args){
    return new BooleanOperator(replaceOpWithFuncName(functionName), args);
  }

  public static LogicalExpression createBooleanOperator(String functionName, LogicalExpression... e) {
    return createBooleanOperator(functionName, Lists.newArrayList(e));
  }

  public static LogicalExpression createByOp(List<LogicalExpression> args, List<String> opTypes) {
    if (args.size() == 1) {
      return args.get(0);
    }

    if (args.size() - 1 != opTypes.size()) {
      throw new IllegalArgumentException("Must receive one more expression than the provided number of operators.");
    }

    LogicalExpression first = args.get(0);
    for (int i = 0; i < opTypes.size(); i++) {
      List<LogicalExpression> l2 = new ArrayList<>();
      l2.add(first);
      l2.add(args.get(i + 1));
      first = createExpression(opTypes.get(i), l2);
    }
    return first;
  }

}
