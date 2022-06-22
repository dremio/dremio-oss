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
import java.util.Map;
import java.util.function.Supplier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class SqlUserDefinedFunctionExpanderRule implements SqlRexConvertlet {

  private final Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier;

  public SqlUserDefinedFunctionExpanderRule(Supplier<SqlValidatorAndToRelContext.Builder> sqlSubQueryConverterBuilderSupplier) {
    this.sqlSubQueryConverterBuilderSupplier = sqlSubQueryConverterBuilderSupplier;
  }

  @Override public RexNode convertCall(SqlRexContext sqlRexContext,
    SqlCall sqlCall) {
    SqlUserDefinedFunction sqlUserDefinedFunction = (SqlUserDefinedFunction) sqlCall.getOperator();

    DremioScalarUserDefinedFunction dremioScalarUserDefinedFunction =
      (DremioScalarUserDefinedFunction) sqlUserDefinedFunction.getFunction();

    SqlValidatorAndToRelContext sqlValidatorAndToRelContext = sqlSubQueryConverterBuilderSupplier.get()
      .withSchemaPath(ImmutableList.of())
      .withUser(dremioScalarUserDefinedFunction.getOwner())
      .withContextualSqlOperatorTable(new FunctionOperatorTable(dremioScalarUserDefinedFunction.getParameters()))
      .build();
    RexBuilder rexBuilder = sqlRexContext.getRexBuilder();
    RelDataTypeFactory relDataTypeFactory = rexBuilder.getTypeFactory();

    Map<String, RexNode> paramNameToRexNode = createParamNameToRexNode(
      sqlRexContext, sqlCall, sqlUserDefinedFunction);

    SqlNode sqlNode = parse(sqlValidatorAndToRelContext.getSqlConverter(), dremioScalarUserDefinedFunction);
    RexNode validatedSqlNode = sqlValidatorAndToRelContext.validateAndConvertFunction(sqlNode, paramNameToRexNode);

    RelDataType targetType = dremioScalarUserDefinedFunction.getReturnType(relDataTypeFactory);

    return rexBuilder.makeCast(targetType, validatedSqlNode, true);
  }

  private SqlNode parse(SqlConverter sqlConverter, DremioScalarUserDefinedFunction udf) {
    SqlNode sqlNode = sqlConverter.parse(udf.getFunctionSql());
    if (sqlNode instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlNode;
      Preconditions.checkState(null == sqlSelect.getFrom());
      Preconditions.checkState(sqlSelect.getSelectList().size() == 1);
      return sqlSelect.getSelectList().get(0);
    } else {
      throw new RuntimeException();
    }
  }

  private Map<String, RexNode> createParamNameToRexNode(
      SqlRexContext sqlRexContext,
      SqlCall sqlCall,
      SqlUserDefinedFunction sqlUserDefinedFunction) {
    Preconditions.checkArgument(sqlUserDefinedFunction.getParamNames().size() ==
      sqlCall.getOperandList().size());

    List<RexNode> argRex = sqlCall.getOperandList().stream()
      .map(sqlRexContext::convertExpression)
      .collect(ImmutableList.toImmutableList());

    List<String> paramNames = sqlUserDefinedFunction.getParamNames();
    ImmutableMap.Builder<String, RexNode> paramNameToRexNode = ImmutableMap.builder();
    for (int i = 0; i < paramNames.size(); i++) {
      paramNameToRexNode.put(paramNames.get(i), argRex.get(i));
    }
    return paramNameToRexNode.build();
  }

  public static class UserDefinedFunctionSqlRexConvertletTable implements SqlRexConvertletTable {
    private final SqlUserDefinedFunctionExpanderRule sqlUserDefinedFunctionExpanderRule;
    public UserDefinedFunctionSqlRexConvertletTable(SqlConverter sqlConverter) {
      this.sqlUserDefinedFunctionExpanderRule =
        new SqlUserDefinedFunctionExpanderRule(() -> SqlValidatorAndToRelContext.builder(sqlConverter));
    }

    @Override public SqlRexConvertlet get(SqlCall sqlCall) {
      SqlOperator operator = sqlCall.getOperator();
      if(operator instanceof SqlUserDefinedFunction) {
        SqlUserDefinedFunction sqlUserDefinedFunction = (SqlUserDefinedFunction) operator;
        if(sqlUserDefinedFunction.getFunction() instanceof DremioScalarUserDefinedFunction) {
          return sqlUserDefinedFunctionExpanderRule;
        }
      }
      return null;
    }
  }
}
