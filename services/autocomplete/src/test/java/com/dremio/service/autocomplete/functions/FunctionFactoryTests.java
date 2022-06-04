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

import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.junit.Test;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.test.GoldenFileTestBuilder;
import com.google.common.collect.ImmutableList;

/**
 * Tests for FunctionFactory that creates a Function from a SqlFunction and serializes the result for golden file testing.
 */
public class FunctionFactoryTests {
  private static final SqlFunctionDictionary sqlFunctionDictionary = new SqlFunctionDictionary(
    new ImmutableList.Builder<SqlFunction>()
      .add(ZeroArgFunction.INSTANCE)
      .add(OneArgBooleanFunction.INSTANCE)
      .add(OneArgNumericFunction.INSTANCE)
      .add(TwoArgNumericFunction.INSTANCE)
      .add(UnstableReturnTypeFunction.INSTANCE)
      .add(VaradicFunction.INSTANCE)
      .add(OverloadedFunction.INSTANCE)
      .build());

  @Test
  public void tests() {
    new GoldenFileTestBuilder<>(FunctionFactoryTests::executeTest)
      .add(ZeroArgFunction.INSTANCE.getName(), ZeroArgFunction.INSTANCE.getName())
      .add(OneArgBooleanFunction.INSTANCE.getName(), OneArgBooleanFunction.INSTANCE.getName())
      .add(OneArgNumericFunction.INSTANCE.getName(), OneArgNumericFunction.INSTANCE.getName())
      .add(TwoArgNumericFunction.INSTANCE.getName(), TwoArgNumericFunction.INSTANCE.getName())
      .add(UnstableReturnTypeFunction.INSTANCE.getName(), UnstableReturnTypeFunction.INSTANCE.getName())
      .add(VaradicFunction.INSTANCE.getName(), VaradicFunction.INSTANCE.getName())
      .add(OverloadedFunction.INSTANCE.getName(), OverloadedFunction.INSTANCE.getName())
      .runTests();
  }

  private static Function executeTest(String functionName) {
    SqlFunction sqlFunction = sqlFunctionDictionary.tryGetValue(functionName).get();
    return FunctionFactory.createFromSqlFunction(
      sqlFunction,
      when(mock(SqlValidator.class).deriveType(isNotNull(), isNotNull()))
        .thenAnswer(answer -> {
          SqlTypeName typeName = answer.getArgument(1, SqlLiteral.class).getTypeName();
          return JavaTypeFactoryImpl.INSTANCE.createSqlType(typeName);
        })
        .getMock(),
      mock(SqlValidatorScope.class));
  }
}
