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
package com.dremio.service.autocomplete.functions;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.service.autocomplete.OperatorTableFactory;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests for FunctionFactory that creates a Function from a SqlFunction and serializes the result for golden file testing.
 */
public final class FunctionFactoryTests {
  private static final SqlOperatorTable OPERATOR_TABLE = OperatorTableFactory.createWithProductionFunctions(ParameterResolverTests.FUNCTIONS);

  @Test
  public void tests() {
    List<String> names = ParameterResolverTests.FUNCTIONS
      .stream()
      .map(sqlOperator -> sqlOperator.getName().toUpperCase())
      .distinct()
      .sorted()
      .collect(Collectors.toList());

    new GoldenFileTestBuilder<>(FunctionFactoryTests::executeTest)
      .addListByRule(names, (name) -> Pair.of(name,name))
      .runTests();
  }

  @Test
  @Ignore
  public void production() {
    List<String> names = OperatorTableFactory.createWithProductionFunctions(ImmutableList.of())
      .getOperatorList()
      .stream()
      .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
      .map(sqlOperator -> sqlOperator.getName().toUpperCase())
      .distinct()
      .sorted()
      .collect(Collectors.toList());

    new GoldenFileTestBuilder<>(FunctionFactoryTests::executeTest)
      .addListByRule(names, (name) -> Pair.of(name,name))
      .runTests();
  }

  private static Function executeTest(String functionName) {
    return FunctionDictionary
      .create(OPERATOR_TABLE
        .getOperatorList()
        .stream()
        .filter(sqlOperator -> sqlOperator instanceof SqlFunction)
        .map(sqlOperator -> (SqlFunction) sqlOperator)
        .filter(sqlFunction -> sqlFunction.getName().equalsIgnoreCase(functionName))
        .map(sqlFunction -> FunctionFactory.create(sqlFunction))
        .collect(Collectors.toList()))
      .tryGetValue(functionName)
      .get();
  }
}
