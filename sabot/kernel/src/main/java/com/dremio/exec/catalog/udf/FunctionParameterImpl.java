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

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

public class FunctionParameterImpl implements FunctionParameter {

  private final int ordinal;
  private final String name;
  private final CompleteType type;

  public FunctionParameterImpl(int ordinal, String name, CompleteType type) {
    this.ordinal = ordinal;
    this.name = Preconditions.checkNotNull(name);
    this.type = Preconditions.checkNotNull(type);
  }

  @Override
  public int getOrdinal() {
    return ordinal;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return CalciteArrowHelper.wrap(type).toCalciteType(relDataTypeFactory, true);
  }

  @Override
  public boolean isOptional() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionParameterImpl that = (FunctionParameterImpl) o;
    return ordinal == that.ordinal && name.equals(that.name) && type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ordinal, name, type);
  }

  public static List<FunctionParameter> createParameters(
      List<UserDefinedFunction.FunctionArg> args) {
    ImmutableList.Builder<FunctionParameter> functionParameters = ImmutableList.builder();
    for (int i = 0; i < args.size(); i++) {
      UserDefinedFunction.FunctionArg arg = args.get(i);
      functionParameters.add(new FunctionParameterImpl(i, arg.getName(), arg.getDataType()));
    }
    return functionParameters.build();
  }
}
