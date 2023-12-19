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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.DremioCompositeSqlOperatorTable;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Generates a baseline for all the functions that are either non deterministic or dynamic.
 * This allows us to better keep track of what functions we are excluding from plan caches, reduction rules, and result cache.
 */
public final class DeterminismAndDynamismTest {
  private static final SabotConfig SABOT_CONFIG = SabotConfig.create();
  private static final ScanResult SCAN_RESULT = ClassPathScanner.fromPrescan(SABOT_CONFIG);
  private static final FunctionImplementationRegistry FUNCTION_IMPLEMENTATION_REGISTRY = FunctionImplementationRegistry.create(
    SABOT_CONFIG,
    SCAN_RESULT);
  private static final SqlOperatorTable OPERATOR_TABLE = DremioCompositeSqlOperatorTable.create(FUNCTION_IMPLEMENTATION_REGISTRY);

  @Test
  public void production() {
    List<String> names = OPERATOR_TABLE
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .filter(sqlOperator -> !sqlOperator.isDeterministic() || sqlOperator.isDynamicFunction())
      .map(sqlOperator -> sqlOperator.getName())
      .distinct()
      .sorted()
      .collect(Collectors.toList());

    GoldenFileTestBuilder.create((String functionName) -> executeTest(functionName))
      .addListByRule(names, (name) -> Pair.of(name,name))
      .runTests();
  }

  private Result executeTest(String functionName) {
    List<SqlFunction> overloads = OPERATOR_TABLE
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> (SqlFunction) sqlOperator)
      .filter(sqlFunction -> sqlFunction.getName().equalsIgnoreCase(functionName))
      .collect(ImmutableList.toImmutableList());

    String name = overloads.get(0).getName();
    boolean isNonDeterministic = overloads.stream().anyMatch(sqlFunction -> !sqlFunction.isDeterministic());
    boolean isDynamic = overloads.stream().anyMatch(sqlFunction -> sqlFunction.isDynamicFunction());

    return new Result(name, isNonDeterministic, isDynamic);
  }

  private static final class Result {
    private final String name;
    private boolean isNonDeterministic;
    private boolean isDynamic;

    public Result(String name, boolean isNonDeterministic, boolean isDynamic) {
      this.name = name;
      this.isNonDeterministic = isNonDeterministic;
      this.isDynamic = isDynamic;
    }

    public String getName() {
      return name;
    }

    public boolean isNonDeterministic() {
      return isNonDeterministic;
    }

    public boolean isDynamic() {
      return isDynamic;
    }
  }
}
