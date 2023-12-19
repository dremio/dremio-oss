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
package com.dremio.exec.store.sys.udf;

import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlOperand;
import com.dremio.exec.planner.sql.SqlOperatorBuilder;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.google.common.collect.ImmutableSet;

public final class UserDefinedFunctionSqlOperator  {
  public static SqlOperator create(UserDefinedFunction userDefinedFunction) {
    return SqlOperatorBuilder
      .name(userDefinedFunction.getName())
      .returnType(CalciteArrowHelper
        .wrap(userDefinedFunction.getReturnType())
        .toCalciteType(JavaTypeFactoryImpl.INSTANCE, true))
      .operandTypes(userDefinedFunction.getFunctionArgsList().stream().map(arg -> {
        String name = arg.getName();
        SqlTypeName sqlTypeName = CalciteArrowHelper
          .wrap(arg.getDataType())
          .toCalciteType(JavaTypeFactoryImpl.INSTANCE, true)
          .getSqlTypeName();
        return SqlOperand.regular(ImmutableSet.of(sqlTypeName));
      }).collect(Collectors.toList()))
      .build();
  }
}
