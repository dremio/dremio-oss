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
package com.dremio.exec.planner.cost.janio;

import java.lang.reflect.Method;

import org.apache.calcite.linq4j.Ord;

/**
 * Common functions for code generation.
 */
public class CodeGeneratorUtil {

  /**
   * Returns e.g. ",\n boolean ignoreNulls".
   */
  static StringBuilder paramList(StringBuilder buff, Method method, int startIndex) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())
        .subList(startIndex, method.getParameterCount())) {
      buff.append(",\n      ").append(t.e.getName()).append(" a").append(t.i);
    }
    return buff;
  }

  /**
   * Returns e.g. ", ignoreNulls".
   */
  static StringBuilder argList(StringBuilder buff, Method method, int startIndex) {
    for (Ord<Class<?>> t : Ord.zip(method.getParameterTypes())
        .subList(startIndex, method.getParameterCount())) {
      buff.append(", a").append(t.i);
    }
    return buff;
  }

  static StringBuilder dispatchMethodName(StringBuilder buff, Method method) {
    return buff.append(method.getName()).append("_");
  }
}
