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
package com.dremio.plugins.elastic.planning.functions;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import org.apache.calcite.rex.RexCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(
    "unused") // sub-classes of ElasticFunction are registered in OPERATOR_MAP using reflection
public final class ElasticFunctions {

  private static final Logger logger = LoggerFactory.getLogger(ElasticFunctions.class);

  // Comparison
  static ElasticFunction NOT_EQUALS = new CompareFunction("<>", "!=", CompareFunction.Type.NEQ);
  static ElasticFunction GREATER = new CompareFunction(">", CompareFunction.Type.GT);
  static ElasticFunction GREATER_OR_EQUAL = new CompareFunction(">=", CompareFunction.Type.GTE);
  static ElasticFunction LESS = new CompareFunction("<", CompareFunction.Type.LT);
  static ElasticFunction LESS_OR_EQUAL = new CompareFunction("<=", CompareFunction.Type.LTE);
  /* Mapping "=" to "==" is fine in groovy & painless as the default is value comparison (not instance comparison) */
  static ElasticFunction EQUALS = new CompareFunction("=", "==", CompareFunction.Type.EQ);

  // Boolean Operations
  static ElasticFunction NOT = unary("not", "!");
  static ElasticFunction OR = new OrFunction();
  static ElasticFunction AND = new AndFunction();

  // Existence Functions
  static ElasticFunction IS_NULL = new NullFunction("is null", " == null");
  static ElasticFunction IS_NOT_NULL = new NullFunction("is not null", "!= null");

  // Flow Functions
  static ElasticFunction CASE = new CaseFunction();

  // Datatype manipulation functions
  static ElasticFunction CAST = new CastFunction();

  private ElasticFunctions() {}

  // String Functions
  static ElasticFunction CHAR_LENGTH = new MethodFunction("char_length", "length");
  static ElasticFunction UPPER = new MethodFunction("upper", "toUpperCase");
  static ElasticFunction LOWER = new MethodFunction("lower", "toLowerCase");
  static ElasticFunction TRIM = new TrimFunction();
  static ElasticFunction CONCAT = binary("||", "+");
  static ElasticFunction CONCAT2 = binary("concat", "+");

  // Numeric Functions
  static ElasticFunction ADD = binary("+");
  static ElasticFunction POWER = new PowerFunction();
  static ElasticFunction DIVIDE = binary("/");
  static ElasticFunction MOD = binary("mod", "%");
  static ElasticFunction MULTIPLY = binary("*");
  static ElasticFunction SUBTRACT = binary("-");
  static ElasticFunction ABS = unary("abs", "Math.abs");
  static ElasticFunction EXP = unary("exp", "Math.exp");
  static ElasticFunction FLOOR = unary("floor", "Math.floor");
  static ElasticFunction CEIL = unary("ceil", "Math.ceil");
  static ElasticFunction LOG = unary("log", "Math.log");
  static ElasticFunction LOG10 = unary("log10", "Math.log10");
  static ElasticFunction SQRT = unary("sqrt", "Math.sqrt");
  static ElasticFunction SIGN = unary("sign", "Math.signum");
  static ElasticFunction COT = unary("cot", "1.0/Math.tan");
  static ElasticFunction ACOS = unary("acos", "Math.acos");
  static ElasticFunction ASIN = unary("asin", "Math.asin");
  static ElasticFunction ATAN = unary("atan", "Math.atan");
  static ElasticFunction DEGREES = unary("degrees", "Math.toDegrees");
  static ElasticFunction RADIANS = unary("radians", "Math.toRadians");
  static ElasticFunction SIN = unary("sin", "Math.sin");
  static ElasticFunction COS = unary("cos", "Math.cos");
  static ElasticFunction TAN = unary("tan", "Math.tan");

  // Date Functions
  static ElasticFunction EXTRACT = new ExtractFunction();

  private static ElasticFunction unary(String calciteName, String elasticName) {
    return new UnaryFunction(calciteName, elasticName);
  }

  private static ElasticFunction binary(String commonName) {
    return new BinaryFunction(commonName);
  }

  private static ElasticFunction binary(String calciteName, String elasticName) {
    return new BinaryFunction(calciteName, elasticName);
  }

  private static final Map<String, ElasticFunction> OPERATOR_MAP;

  static {
    ImmutableMap.Builder<String, ElasticFunction> builder =
        ImmutableMap.<String, ElasticFunction>builder();
    for (Field field : ElasticFunctions.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers())
          && ElasticFunction.class.isAssignableFrom(field.getType())) {
        try {
          ElasticFunction function = ElasticFunction.class.cast(field.get(null));
          builder.put(function.getDremioName().toLowerCase(), function);
        } catch (Exception e) {
          logger.error("Failure while trying to access function.", e);
        }
      }
    }
    OPERATOR_MAP = builder.build();
  }

  public static ElasticFunction getFunction(RexCall call) {
    return OPERATOR_MAP.get(call.getOperator().getName().toLowerCase());
  }

  public static void main(String[] args) {
    System.out.println(OPERATOR_MAP);
  }
}
