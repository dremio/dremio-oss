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
package com.dremio.exec.planner.sql.handlers.direct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.udf.FunctionParameterImpl;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.SqlValidatorAndToRelContext;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.SqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlColumnPolicyPair;
import com.dremio.exec.planner.sql.parser.SqlCreateFunction;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * CreateFunctionHandler
 */
public class CreateFunctionHandler extends SimpleDirectHandler {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CreateFunctionHandler.class);
  private static final RelDataTypeFactory TYPE_FACTORY = SqlTypeFactoryImpl.INSTANCE;
  private static final RexBuilder REX_BUILDER = new DremioRexBuilder(TYPE_FACTORY);

  public static final String DUPLICATE_PARAMETER_ERROR_MSG = "Parameter name %s appears more than once";

  private final QueryContext context;

  public CreateFunctionHandler(QueryContext context) {
    this.context = context;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final Catalog catalog = context.getCatalog();
    final SqlCreateFunction createFunction = SqlNodeUtil.unwrap(sqlNode, SqlCreateFunction.class);
    final NamespaceKey functionKey = catalog.resolveSingle(createFunction.getPath());
    if (createFunction.isIfNotExists() && createFunction.shouldReplace()) {
      throw UserException.validationError().message("Cannot create a user-defined function with both IF NOT EXISTS and OR REPLACE").build(logger);
    }

    boolean exists = false;
    try {
      exists = catalog.getFunction(functionKey) != null;
    } catch (Exception ignored) {}
    if (exists && !createFunction.shouldReplace()) {
      if (createFunction.isIfNotExists()) {
        return Collections.singletonList(SimpleCommandResult.successful(String.format("Function, %s, is not created as it already exists", functionKey)));
      }
      throw UserException.validationError().message(String.format("The function with a key, %s, already exists", functionKey)).build(logger);
    }

    final List<SqlNode> argList = createFunction.getFieldList().getList();
    final List<UserDefinedFunction.FunctionArg> convertedArgList = new ArrayList<>();
    final Map<String, RexNode> inputMap = new HashMap<>();

    for (int i = 0 ; i < argList.size() ; i++) {
      List<SqlNode> arg = ((SqlNodeList) argList.get(i)).getList();
      String name = arg.get(0).toString();
      SqlDataTypeSpec dataTypeSpec = (SqlDataTypeSpec) arg.get(1);
      Field field = SqlHandlerUtil.fieldFromSqlColDeclaration(
        TYPE_FACTORY,
        new SqlColumnDeclaration(
          SqlParserPos.ZERO,
          new SqlColumnPolicyPair(SqlParserPos.ZERO, new SqlIdentifier(name, SqlParserPos.ZERO), null),
          dataTypeSpec,
          null),
        sql);
      CompleteType completeType = CompleteType.fromField(field);

      if (inputMap.containsKey(name)) {
        throw UserException.validationError().message(String.format(DUPLICATE_PARAMETER_ERROR_MSG, name)).build(logger);
      }

      inputMap.put(name, REX_BUILDER.makeInputRef(
        CalciteArrowHelper.toCalciteType(field, TYPE_FACTORY, true), i));
      convertedArgList.add(new UserDefinedFunction.FunctionArg(name, completeType));
    }

    final SqlNode expression = extractScalarExpression(createFunction.getExpression());
    RexNode parsedExpression = validate(expression, convertedArgList, inputMap);
    Field returnField = SqlHandlerUtil.fieldFromSqlColDeclaration(
      TYPE_FACTORY,
      new SqlColumnDeclaration(
        SqlParserPos.ZERO,
        new SqlColumnPolicyPair(SqlParserPos.ZERO, new SqlIdentifier("return", SqlParserPos.ZERO), null),
        createFunction.getReturnType(),
        null),
      sql);
    final RelDataType expectedReturnType = CalciteArrowHelper.toCalciteType(returnField, TYPE_FACTORY, true);
    final RelDataType returnDataType = parsedExpression.getType();
    final RelDataType returnRowType = TYPE_FACTORY.createStructType(ImmutableList.of(returnDataType), ImmutableList.of("return"));
    final RelDataType expectedReturnRowType = TYPE_FACTORY.createStructType(ImmutableList.of(expectedReturnType), ImmutableList.of("return"));
    if (MoreRelOptUtil.areRowTypesCompatible(returnRowType, expectedReturnRowType, false, true)) {
      CompleteType completeReturnType = CompleteType.fromField(returnField);
      UserDefinedFunction newUdf = new UserDefinedFunction(functionKey.toString(), createFunction.getExpression()
        .toSqlString(CalciteSqlDialect.DEFAULT).getSql(),
        completeReturnType,
        convertedArgList);
      if (exists) {
        catalog.updateFunction(functionKey, newUdf);
        return Collections.singletonList(SimpleCommandResult.successful(String.format("Function, %s, is updated.", functionKey)));
      } else {
        catalog.createFunction(functionKey, newUdf);
        return Collections.singletonList(SimpleCommandResult.successful(String.format("Function, %s, is created.", functionKey)));
      }
    }
    throw UserException.validationError().message("Row types are different.\nDefined: %s\nActual: %s", expectedReturnRowType, returnDataType).build(logger);
  }

  private RexNode validate(SqlNode expressionNode,
      List<UserDefinedFunction.FunctionArg> args,
      Map<String, RexNode> inputMap) {
    SqlConverter converter = new SqlConverter(
      context.getPlannerSettings(),
      context.getOperatorTable(),
      context,
      context.getMaterializationProvider(),
      context.getFunctionRegistry(),
      context.getSession(),
      null,
      context.getCatalog(),
      context.getSubstitutionProviderFactory(),
      context.getConfig(),
      context.getScanResult(),
      context.getRelMetadataQuerySupplier());

    final SqlValidatorAndToRelContext sqlValidatorAndToRelContext = SqlValidatorAndToRelContext.builder(converter)
      .withContextualSqlOperatorTable(
        new FunctionOperatorTable(FunctionParameterImpl.createParameters(args)))
      .build();

    return sqlValidatorAndToRelContext.validateAndConvertFunction(expressionNode, inputMap);
  }

  private SqlNode extractScalarExpression(SqlNode expression) {
    if (expression instanceof SqlSelect) {
      List<SqlNode> selectList = ((SqlSelect) expression).getSelectList().getList();
      if (selectList.size() == 1) {
        return selectList.get(0);
      } else {
        throw UserException.unsupportedError().message("Returning a table is not currently supported").build(logger);
      }
    } else {
      return expression;
    }
  }
}
