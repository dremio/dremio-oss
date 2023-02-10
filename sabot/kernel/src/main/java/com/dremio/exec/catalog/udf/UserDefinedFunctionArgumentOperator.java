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
package com.dremio.exec.catalog.udf;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlBaseContextVariable;

import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;

/**
 * This is a work around since for creating a different rex node type
 */
public class UserDefinedFunctionArgumentOperator {
  public static List<SqlOperator> createArgumentOperator(
    String udfName,
    List<FunctionParameter> parameters) {

    return parameters.stream()
      .map(p ->
        new ScalarArgumentOperator(
          udfName + p.getName(),
          p.getName(),
          p.getType(SqlTypeFactoryImpl.INSTANCE)))
      .collect(ImmutableList.toImmutableList());
  }

  public abstract static class ArgumentOperator extends SqlBaseContextVariable {
    private final RelDataType returnRelDataType;
    public ArgumentOperator(String name,
      RelDataType returnRelDataType, SqlFunctionCategory category) {
      super(name, (sqlOperatorBinding)-> returnRelDataType, category);
      this.returnRelDataType = returnRelDataType;
    }

    public RelDataType getReturnRelDataType(){
      return returnRelDataType;
    }
  }
}

class ScalarArgumentOperator extends UserDefinedFunctionArgumentOperator.ArgumentOperator {
  public final String namePath;
  public ScalarArgumentOperator(String namePath, String name, RelDataType relDataType) {
    super(name, relDataType, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.namePath = namePath;
  }

  public String getNamePath() {
    return namePath;
  }
}
