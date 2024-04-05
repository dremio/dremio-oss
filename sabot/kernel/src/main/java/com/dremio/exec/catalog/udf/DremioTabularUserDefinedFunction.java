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

import com.dremio.exec.catalog.CatalogIdentity;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

/** Wraps a UserDefinedFunction to meet the TableFunction API */
public final class DremioTabularUserDefinedFunction
    implements DremioUserDefinedFunction, TableFunction {
  private final CatalogIdentity owner;
  private final UserDefinedFunction userDefinedFunction;

  public DremioTabularUserDefinedFunction(
      CatalogIdentity owner, UserDefinedFunction userDefinedFunction) {
    this.owner = owner;
    this.userDefinedFunction = userDefinedFunction;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments) {
    return CalciteArrowHelper.wrap(userDefinedFunction.getReturnType())
        .toCalciteType(typeFactory, true);
  }

  @Override
  public Type getElementType(List<Object> arguments) {
    return Object[].class;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return FunctionParameterImpl.createParameters(userDefinedFunction.getFunctionArgsList());
  }

  @Override
  public String getFunctionSql() {
    return userDefinedFunction.getFunctionSql();
  }

  @Override
  public CatalogIdentity getOwner() {
    return owner;
  }

  @Override
  public String getName() {
    return userDefinedFunction.getName();
  }
}
