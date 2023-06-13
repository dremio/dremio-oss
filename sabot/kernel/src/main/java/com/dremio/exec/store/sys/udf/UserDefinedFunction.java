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

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.dremio.common.expression.CompleteType;

public final class UserDefinedFunction {
  private final String name;
  private final String functionSql;
  private final CompleteType returnType;
  private final List<FunctionArg> functionArgList;
  private final List<String> fullPath;
  private final byte[] serializedFunctionPlan;

  @Nullable
  private Timestamp createdAt;
  @Nullable
  private Timestamp modifiedAt;

  public UserDefinedFunction(
    String name,
    String functionSql,
    CompleteType returnType,
    List<FunctionArg> functionArgList,
    List<String> fullPath,
    byte[] serializedFunctionPlan,
    Timestamp createdAt,
    Timestamp modifiedAt) {
    this.name = name;
    this.functionSql = functionSql;
    this.returnType = returnType;
    this.functionArgList = functionArgList;
    this.fullPath = fullPath;
    this.serializedFunctionPlan = serializedFunctionPlan;
    this.createdAt = createdAt;
    this.modifiedAt = modifiedAt;
  }

  @Nullable
  public Timestamp getCreatedAt() {
    return createdAt;
  }

  @Nullable
  public Timestamp getModifiedAt() {
    return modifiedAt;
  }

  public String getName() {
    return name;
  }

  public String getFunctionSql(){
    return functionSql;
  }

  public CompleteType getReturnType(){
    return returnType;
  }

  public List<FunctionArg> getFunctionArgsList() {
    return functionArgList;
  }

  public List<String> getFullPath() {
    return fullPath;
  }

  public byte[] getSerializedFunctionPlan() {
    return serializedFunctionPlan;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UserDefinedFunction that = (UserDefinedFunction) o;
    return name.equals(that.name) && functionSql.equals(that.functionSql) &&
      returnType.equals(that.returnType) && Objects.equals(functionArgList, that.functionArgList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, functionSql, returnType, functionArgList);
  }

  public static class FunctionArg {
    private final CompleteType dataType;
    private final String name;
    private final String defaultExpression;

    public FunctionArg(String name, CompleteType dataType, String defaultExpression) {
      this.dataType = dataType;
      this.name = name;
      this.defaultExpression = defaultExpression;
    }

    public String getName() {
      return name;
    }

    public CompleteType getDataType() {
      return dataType;
    }

    public String getDefaultExpression() {
      return defaultExpression;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FunctionArg that = (FunctionArg) o;
      return this.dataType.equals(that.dataType)
        && name.equals(that.name)
        && defaultExpression.equals(that.defaultExpression);
    }

    @Override
    public String toString() {
      return String.format("{\"name\": \"%s\", \"type\": \"%s\", \"default_expr\": \"%s\"}", name, dataType.toString(), defaultExpression);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataType, name, defaultExpression);
    }
  }
}
