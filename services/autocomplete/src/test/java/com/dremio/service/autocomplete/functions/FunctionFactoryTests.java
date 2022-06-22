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

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlOperator;
import org.junit.Test;

import com.dremio.test.GoldenFileTestBuilder;

/**
 * Tests for FunctionFactory that creates a Function from a SqlFunction and serializes the result for golden file testing.
 */
public final class FunctionFactoryTests {
  private static final FunctionDictionary functionDictionary = FunctionDictionaryFactory.createWithProductionFunctions(ParameterResolverTests.FUNCTIONS);

  @Test
  public void tests() {
    GoldenFileTestBuilder<String, Function> builder = new GoldenFileTestBuilder<>(FunctionFactoryTests::executeTest);
    Set<String> functionNames = new TreeSet<>();
    for (SqlOperator sqlFunction : ParameterResolverTests.FUNCTIONS) {
      functionNames.add(sqlFunction.getName());
    }

    for (String functionName : functionNames) {
      builder.add(functionName, functionName);
    }

    builder.runTests();
  }

  @Test
  public void production() {
    GoldenFileTestBuilder<String, Function> builder = new GoldenFileTestBuilder<>(FunctionFactoryTests::executeTest);
    for (String name : functionDictionary.getKeys().stream().sorted().collect(Collectors.toList())) {
      builder.add(name, name);
    }

    builder.runTests();
  }

  private static Function executeTest(String functionName) {
    return functionDictionary.tryGetValue(functionName).get();
  }
}
