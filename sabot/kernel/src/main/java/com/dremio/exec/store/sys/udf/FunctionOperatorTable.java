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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import com.dremio.exec.catalog.udf.UserDefinedFunctionArgumentOperator;
import com.google.common.collect.Iterables;

public class FunctionOperatorTable implements SqlOperatorTable {
  private final List<SqlOperator> functionParameterList;

  public FunctionOperatorTable(String udfName,
    List<FunctionParameter> functionParameters) {
    this.functionParameterList = functionParameters
      .stream()
      .map(parameter -> UserDefinedFunctionArgumentOperator.createArgumentOperator(udfName, parameter))
      .collect(Collectors.toList());
  }

  @Override public void lookupOperatorOverloads(
      SqlIdentifier opName,
      SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    if (category != SqlFunctionCategory.USER_DEFINED_FUNCTION && category != null) {
      return;
    } else if(syntax != SqlSyntax.FUNCTION
      || opName == null
      || opName.names.size() != 1) {
      return;
    }

    String name = Iterables.getOnlyElement(opName.names);
    functionParameterList.stream()
        .filter(param -> nameMatcher.matches(name, param.getName()))
        .forEach(operatorList::add);
  }

  @Override public List<SqlOperator> getOperatorList() {
    return functionParameterList;
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionOperatorTable that = (FunctionOperatorTable) o;
    return functionParameterList.equals(that.functionParameterList);
  }

  @Override public int hashCode() {
    return Objects.hash(functionParameterList);
  }
}
