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
package com.dremio.service.functions;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.exec.planner.sql.NonCacheableFunctionDetector;
import com.dremio.exec.planner.sql.SqlFunctionImpl;
import com.dremio.test.GoldenFileTestBuilder;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/**
 * Generates a baseline for all the functions that are either non deterministic or dynamic. This
 * allows us to better keep track of what functions we are excluding from plan caches, reduction
 * rules, and result cache.
 */
public final class DeterminismAndDynamismTest {
  private static final SabotConfig SABOT_CONFIG = SabotConfig.create();
  private static final ScanResult SCAN_RESULT = ClassPathScanner.fromPrescan(SABOT_CONFIG);
  private static final FunctionImplementationRegistry FUNCTION_IMPLEMENTATION_REGISTRY =
      FunctionImplementationRegistry.create(SABOT_CONFIG, SCAN_RESULT);
  private static final SqlOperatorTable OPERATOR_TABLE =
      DremioCompositeSqlOperatorTable.create(FUNCTION_IMPLEMENTATION_REGISTRY);

  @Test
  public void production() {
    List<String> names =
        OPERATOR_TABLE.getOperatorList().stream()
            .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
            .filter(
                sqlOperator -> !sqlOperator.isDeterministic() || sqlOperator.isDynamicFunction())
            .map(sqlOperator -> sqlOperator.getName())
            .distinct()
            .sorted()
            .collect(Collectors.toList());

    GoldenFileTestBuilder.create((String functionName) -> executeTest(functionName))
        .addListByRule(names, (name) -> Pair.of(name, name))
        .runTests();
  }

  private Result executeTest(String functionName) {
    List<SqlFunction> overloads =
        OPERATOR_TABLE.getOperatorList().stream()
            .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
            .map(sqlOperator -> (SqlFunction) sqlOperator)
            .filter(sqlFunction -> sqlFunction.getName().equalsIgnoreCase(functionName))
            .collect(ImmutableList.toImmutableList());

    List<ResultItem> resultItems =
        overloads.stream()
            .map(
                overload -> {
                  String className = overload.getClass().getSimpleName();
                  if (overload instanceof SqlFunctionImpl) {
                    SqlFunctionImpl sqlFunctionImpl = (SqlFunctionImpl) overload;
                    className = sqlFunctionImpl.getSource() + " - " + className;
                  }

                  boolean isNonDeterministic = !overload.isDeterministic();
                  boolean isDynamic = overload.isDynamicFunction();
                  boolean isReflectionAllowed = isReflectionAllowed(overload);
                  boolean isPlanCacheAllowed = isPlanCacheAllowed(overload);

                  return new ResultItem(
                      className,
                      isNonDeterministic,
                      isDynamic,
                      isReflectionAllowed,
                      isPlanCacheAllowed);
                })
            .collect(Collectors.toList());
    return new Result(overloads.get(0).getName(), resultItems);
  }

  @JsonPropertyOrder({
    "className",
    "isNonDeterministic",
    "isDynamic",
    "isReflectionAllowed",
    "isPlanCacheAllowed"
  })
  private static final class ResultItem {
    private final String className;
    private final boolean isNonDeterministic;
    private final boolean isDynamic;
    private final boolean isReflectionAllowed;
    private final boolean isPlanCacheAllowed;

    public ResultItem(
        String className,
        boolean isNonDeterministic,
        boolean isDynamic,
        boolean isReflectionAllowed,
        boolean isPlanCacheAllowed) {
      this.className = className;
      this.isNonDeterministic = isNonDeterministic;
      this.isDynamic = isDynamic;
      this.isReflectionAllowed = isReflectionAllowed;
      this.isPlanCacheAllowed = isPlanCacheAllowed;
    }

    public String getClassName() {
      return className;
    }

    public boolean isNonDeterministic() {
      return isNonDeterministic;
    }

    public boolean isDynamic() {
      return isDynamic;
    }

    public boolean isReflectionAllowed() {
      return isReflectionAllowed;
    }

    public boolean isPlanCacheAllowed() {
      return isPlanCacheAllowed;
    }
  }

  private static final class Result {
    private final String name;
    private final List<ResultItem> overloads;

    private Result(String name, List<ResultItem> overloads) {
      this.name = name;
      this.overloads = overloads;
    }

    public List<ResultItem> getOverloads() {
      return overloads;
    }

    public String getName() {
      return name;
    }
  }

  private static boolean isReflectionAllowed(SqlOperator sqlOperator) {
    return NonCacheableFunctionDetector.detect(sqlOperator).isReflectionAllowed();
  }

  private static boolean isPlanCacheAllowed(SqlOperator sqlOperator) {
    return NonCacheableFunctionDetector.detect(sqlOperator).isPlanCacheable();
  }
}
