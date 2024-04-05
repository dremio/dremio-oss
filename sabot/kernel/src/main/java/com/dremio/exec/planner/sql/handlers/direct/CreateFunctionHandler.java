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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.types.Types;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.udf.CorrelatedUdfDetector;
import com.dremio.exec.catalog.udf.FunctionParameterImpl;
import com.dremio.exec.catalog.udf.UserDefinedFunctionCatalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.parser.DremioSqlColumnDeclaration;
import com.dremio.exec.planner.sql.parser.SqlColumnPolicyPair;
import com.dremio.exec.planner.sql.parser.SqlComplexDataTypeSpec;
import com.dremio.exec.planner.sql.parser.SqlComplexDataTypeSpecWithDefault;
import com.dremio.exec.planner.sql.parser.SqlCreateFunction;
import com.dremio.exec.planner.sql.parser.SqlFunctionReturnType;
import com.dremio.exec.planner.sql.parser.SqlReturnField;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.store.sys.udf.FunctionOperatorTable;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.exec.store.sys.udf.UserDefinedFunctionPlanSerde;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/** CreateFunctionHandler */
public final class CreateFunctionHandler extends SimpleDirectHandler {
  private static final boolean ALLOW_DEFAULT_EXPRESSIONS = false;
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(CreateFunctionHandler.class);
  private static final RelDataTypeFactory TYPE_FACTORY = SqlTypeFactoryImpl.INSTANCE;
  private static final String DUPLICATE_PARAMETER_ERROR_MSG =
      "Parameter name %s appears more than once";
  private static final Double ONE = 1.0;

  private final QueryContext context;
  private final UserDefinedFunctionCatalog userDefinedFunctionCatalog;

  public CreateFunctionHandler(QueryContext context) {
    this.context = context;
    this.userDefinedFunctionCatalog = context.getUserDefinedFunctionCatalog();
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    SqlCreateFunction createFunction = SqlNodeUtil.unwrap(sqlNode, SqlCreateFunction.class);
    SimpleCommandResult result = toResultImplementation(sql, createFunction);
    if (!result.ok) {
      throw UserException.validationError().message(result.summary).build();
    }

    return Collections.singletonList(result);
  }

  private SimpleCommandResult toResultImplementation(String sql, SqlCreateFunction createFunction)
      throws Exception {
    if (createFunction.isIfNotExists() && createFunction.shouldReplace()) {
      return SimpleCommandResult.fail(
          "Cannot create a user-defined function with both IF NOT EXISTS and OR REPLACE");
    }

    Catalog catalog = context.getCatalog();
    NamespaceKey functionKey = catalog.resolveSingle(createFunction.getPath());
    boolean exists = doesFunctionExist(userDefinedFunctionCatalog, functionKey);
    if (exists && !createFunction.shouldReplace()) {
      return createFunction.isIfNotExists()
          ? SimpleCommandResult.successful(
              String.format("Function, %s, is not created as it already exists", functionKey))
          : SimpleCommandResult.fail("The function with a key, %s, already exists", functionKey);
    }

    UserDefinedFunction newUdf = extractUdf(context, createFunction, sql, functionKey);

    String action;
    if (exists) {
      action = "updated";
      userDefinedFunctionCatalog.updateFunction(functionKey, newUdf);
    } else {
      action = "created";
      userDefinedFunctionCatalog.createFunction(functionKey, newUdf);
    }

    return SimpleCommandResult.successful(
        String.format("Function, %s, is %s.", functionKey, action));
  }

  private static boolean doesFunctionExist(
      UserDefinedFunctionCatalog userDefinedFunctionCatalog, NamespaceKey functionKey) {
    boolean exists;
    try {
      exists = userDefinedFunctionCatalog.getFunction(functionKey) != null;
    } catch (Exception ignored) {
      exists = false;
    }

    return exists;
  }

  private static UserDefinedFunction extractUdf(
      QueryContext context,
      SqlCreateFunction createFunction,
      String sql,
      NamespaceKey functionKey) {
    /**
     * Calcite has a bug where SqlToRelConverter will mutate the SQL DOM. In some scenarios like:
     * DX-64420 This leads to a query that can no longer be executed. The solution is to serialize
     * the query before converting to a rel and saving it. So we have to keep this code at the top
     * of the method.
     */
    // We really need to find a better way to serialize the query so that it roundtrips with the
    // parser.
    // For some reason the with statement needs a parens to roundtrip.
    boolean forceParens = createFunction.getExpression() instanceof SqlWith;
    String normalizedQuery =
        toQuery(createFunction.getExpression())
            // Query doesn't reparse if you force parens
            .toSqlString(CalciteSqlDialect.DEFAULT, forceParens)
            .getSql()
            // For some reason identifiers are escape with a ` instead of a "
            .replace('`', '"');

    SqlConverter sqlConverter = createConverter(context);
    List<UserDefinedFunction.FunctionArg> arguments =
        extractFunctionArguments(createFunction, sql, sqlConverter);

    // This is the fun part ... the validator will only swap out identifiers with function calls if
    // they are unquoted
    // But the serialization of the query decides to add quotes for some identifiers.
    for (UserDefinedFunction.FunctionArg arg : arguments) {
      String quotedName = '"' + arg.getName() + '"';
      normalizedQuery =
          normalizedQuery.replaceAll(
              Pattern.quote(quotedName), Matcher.quoteReplacement(arg.getName()));
    }

    // Reparse the query just to make sure that the normalized query roundtrips.
    // If it fails here, then it will fail when we try to execute the UDF from just the sql text.
    try {
      parse(normalizedQuery, context);
    } catch (Exception ex) {
      throw UserException.systemError(ex)
          .message("Failed to deserialize the UDF trying to be created: " + sql)
          .buildSilently();
    }

    RelNode functionPlan = extractFunctionPlan(context, createFunction, arguments);

    if (createFunction.isTabularFunction()) {
      if (CorrelatedUdfDetector.hasCorrelatedUdf(functionPlan)) {
        throw UserException.validationError()
            .message("Tabular UDFs must not be correlated.")
            .build(logger);
      }
    } else {
      RelMetadataQuery relMetadataQuery = context.getRelMetadataQuerySupplier().get();
      Double maxRowCount = relMetadataQuery.getMaxRowCount(functionPlan);
      Double minRowCount = relMetadataQuery.getMinRowCount(functionPlan);
      if (!ONE.equals(maxRowCount) || !ONE.equals(minRowCount)) {
        throw UserException.validationError()
            .message("Scalar UDFs must return 1 row")
            .build(logger);
      }
    }

    final Pair<RelDataType, Field> expectedReturnRowTypeAndField =
        extractExpectedRowTypeAndReturnField(createFunction.getReturnType(), sql);

    final RelDataType expectedReturnRowType = expectedReturnRowTypeAndField.left;
    final RelDataType actualReturnRowType = functionPlan.getRowType();

    if (expectedReturnRowType.getFieldCount() != actualReturnRowType.getFieldCount()) {
      throw UserException.validationError()
          .message(
              "Number of columns mismatched \nDefined: %s\nActual: %s",
              expectedReturnRowType.getFieldCount(), actualReturnRowType.getFieldCount())
          .build(logger);
    }

    if (!MoreRelOptUtil.areRowTypesCompatible(
        expectedReturnRowType, actualReturnRowType, false, true)) {
      throw UserException.validationError()
          .message(
              "Row types are different.\nDefined: %s\nActual: %s",
              expectedReturnRowType, actualReturnRowType)
          .build(logger);
    }

    CompleteType completeReturnType = CompleteType.fromField(expectedReturnRowTypeAndField.right);

    UserDefinedFunction uncatalogedUserDefinedFunction =
        new UserDefinedFunction(
            functionKey.toString(),
            normalizedQuery,
            completeReturnType,
            arguments,
            functionKey.getPathComponents(),
            null,
            null,
            null);

    // The Serialization of the UDF plan is only used in an optimization at this point,
    // so it's optional whether this plan needs to be serializable or not.
    // For reflection matching and plan cache we use the "normalized" query plan that runs
    // "UDF Expansion" as a substep, which ensures there are no UDFs in the normalized plan.
    byte[] serializedFunctionPlan;
    try {
      serializedFunctionPlan =
          UserDefinedFunctionPlanSerde.serialize(
              functionPlan, context, uncatalogedUserDefinedFunction);
    } catch (Exception ex) {
      if (!context.getPlannerSettings().isUnserializableUdfAllowed()) {
        throw new UnsupportedOperationException(
            "Failed to serialize the UDF trying to be created: "
                + sql
                + "\n"
                + "This means that the UDF can not be used in contexts that require serialization."
                + "If this is not a concern for you, then set the config 'udf.enable_unserializable_functions' to true.",
            ex);
      } else {
        serializedFunctionPlan = new byte[] {};
      }
    }

    try {
      UserDefinedFunctionPlanSerde.deserialize(
          serializedFunctionPlan,
          functionPlan.getCluster(),
          context,
          uncatalogedUserDefinedFunction);
    } catch (Exception ex) {
      if (!context.getPlannerSettings().isUnserializableUdfAllowed()) {
        throw new UnsupportedOperationException(
            "Failed to deserialize the UDF trying to be created: "
                + sql
                + "\n"
                + "This means that the UDF can not be used in contexts that require serialization."
                + "If this is not a concern for you, then set the config 'udf.enable_unserializable_functions' to true.",
            ex);
      }
    }

    UserDefinedFunction udf =
        new UserDefinedFunction(
            functionKey.toString(),
            normalizedQuery,
            completeReturnType,
            arguments,
            functionKey.getPathComponents(),
            serializedFunctionPlan,
            null,
            null);
    return udf;
  }

  private static List<UserDefinedFunction.FunctionArg> extractFunctionArguments(
      SqlCreateFunction createFunction, String sql, SqlConverter sqlConverter) {
    List<SqlNode> argList = createFunction.getFieldList().getList();
    Set<String> distinctArgName = new HashSet<>();
    List<UserDefinedFunction.FunctionArg> convertedArgs = new ArrayList<>();
    for (int i = 0; i < argList.size(); i++) {
      List<SqlNode> arg = ((SqlNodeList) argList.get(i)).getList();

      // Extract the name
      String name = arg.get(0).toString();
      if (!distinctArgName.add(name)) {
        throw UserException.validationError()
            .message(String.format(DUPLICATE_PARAMETER_ERROR_MSG, name))
            .buildSilently();
      }

      // Extract the type
      SqlComplexDataTypeSpecWithDefault dataTypeSpec =
          (SqlComplexDataTypeSpecWithDefault) arg.get(1);
      Field field =
          SqlHandlerUtil.fieldFromSqlColDeclaration(
              TYPE_FACTORY,
              new DremioSqlColumnDeclaration(
                  SqlParserPos.ZERO,
                  new SqlColumnPolicyPair(
                      SqlParserPos.ZERO, new SqlIdentifier(name, SqlParserPos.ZERO), null),
                  dataTypeSpec,
                  null),
              sql);
      CompleteType completeType = CompleteType.fromField(field);
      RelDataType relDataType = CalciteArrowHelper.toCalciteType(field, TYPE_FACTORY, true);

      // Extract the default expression
      SqlNode defaultExpression = null;
      if (dataTypeSpec.getDefaultExpression() != null) {
        // For now we are disabling default expressions,
        // since we haven't updated the resolver to resolve optional parameters
        // Basically we don't want a user to be able to create a function that they can't execute
        if (!ALLOW_DEFAULT_EXPRESSIONS) {
          throw UserException.validationError()
              .message(
                  "Default Expression Is Not Supported Yet.\n"
                      + "Create two overloads of the function (one with and one without the expression instead.")
              .buildSilently();
        }

        defaultExpression =
            extractScalarExpressionFromDefaultExpression(dataTypeSpec.getDefaultExpression());
        RelDataType actualType = getTypeFromSqlNode(sqlConverter, defaultExpression);
        RelDataType expectedType = relDataType;
        if (!MoreRelOptUtil.checkFieldTypesCompatibility(expectedType, actualType, true, false)) {
          throw UserException.validationError()
              .message(
                  String.format(
                      "Default expression type, %s, is not compatible with argument type, %s",
                      actualType, expectedType))
              .build(logger);
        }
      }

      UserDefinedFunction.FunctionArg convertedArg =
          new UserDefinedFunction.FunctionArg(
              name,
              completeType,
              defaultExpression == null
                  ? null
                  : defaultExpression.toSqlString(CalciteSqlDialect.DEFAULT).getSql());
      convertedArgs.add(convertedArg);
    }

    return convertedArgs;
  }

  private static Pair<RelDataType, Field> extractExpectedRowTypeAndReturnField(
      SqlFunctionReturnType returnType, String sql) {
    final RelDataType expectedReturnRowType;
    final Field returnField;
    if (returnType.isTabular()) {
      List<RelDataType> returnFields = new ArrayList<>();
      List<String> names = new ArrayList<>();
      List<Field> fields = new ArrayList<>();
      for (SqlNode columnDef : returnType.getTabularReturnType()) {
        SqlReturnField sqlReturnField = (SqlReturnField) columnDef;
        SqlIdentifier name = sqlReturnField.getName();
        SqlComplexDataTypeSpec type = sqlReturnField.getType();
        names.add(name.toString());
        Field field =
            SqlHandlerUtil.fieldFromSqlColDeclaration(
                TYPE_FACTORY,
                new DremioSqlColumnDeclaration(
                    SqlParserPos.ZERO,
                    new SqlColumnPolicyPair(SqlParserPos.ZERO, name, null),
                    type,
                    null),
                sql);
        fields.add(field);

        returnFields.add(CalciteArrowHelper.toCalciteType(field, TYPE_FACTORY, true));
      }

      returnField =
          new Field(
              "return",
              new FieldType(
                  true,
                  MajorTypeHelper.getArrowTypeForMajorType(
                      Types.optional(TypeProtos.MinorType.STRUCT)),
                  null),
              fields);
      expectedReturnRowType =
          TYPE_FACTORY.createTypeWithNullability(
              TYPE_FACTORY.createStructType(returnFields, names), true);
    } else {
      returnField =
          SqlHandlerUtil.fieldFromSqlColDeclaration(
              TYPE_FACTORY,
              new DremioSqlColumnDeclaration(
                  SqlParserPos.ZERO,
                  new SqlColumnPolicyPair(
                      SqlParserPos.ZERO, new SqlIdentifier("return", SqlParserPos.ZERO), null),
                  returnType.getScalarReturnType(),
                  null),
              sql);
      expectedReturnRowType =
          TYPE_FACTORY.createStructType(
              ImmutableList.of(CalciteArrowHelper.toCalciteType(returnField, TYPE_FACTORY, true)),
              ImmutableList.of("return"));
    }

    return Pair.of(expectedReturnRowType, returnField);
  }

  private static SqlNode extractScalarExpressionFromDefaultExpression(SqlNode defaultExpression) {
    if (!(defaultExpression instanceof SqlSelect)) {
      return defaultExpression;
    }

    // We need to unwrap the default expression to extract out the single column
    List<SqlNode> selectList = ((SqlSelect) defaultExpression).getSelectList().getList();
    if (selectList.size() != 1) {
      throw UserException.unsupportedError()
          .message("Returning a table is not currently supported")
          .build(logger);
    }

    return selectList.get(0);
  }

  private static RelDataType getTypeFromSqlNode(SqlConverter converter, SqlNode expressionNode) {
    Project project =
        (Project)
            converter
                .getExpansionSqlValidatorAndToRelContextBuilderFactory()
                .builder()
                .disallowSubqueryExpansion()
                .build()
                .validateAndConvertForExpression(expressionNode);

    assert project.getProjects().size() == 1;

    return project.getProjects().get(0).getType();
  }

  private static RelNode extractFunctionPlan(
      QueryContext queryContext,
      SqlCreateFunction createFunction,
      List<UserDefinedFunction.FunctionArg> args) {
    return createConverter(queryContext)
        .getExpansionSqlValidatorAndToRelContextBuilderFactory()
        .builder()
        .withContextualSqlOperatorTable(
            new FunctionOperatorTable(
                createFunction.getFullName(), FunctionParameterImpl.createParameters(args)))
        .disallowSubqueryExpansion()
        .build()
        .validateAndConvertForExpression(createFunction.getExpression());
  }

  private static SqlConverter createConverter(QueryContext context) {
    return new SqlConverter(
        context.getPlannerSettings(),
        context.getOperatorTable(),
        context,
        context.getMaterializationProvider(),
        context.getFunctionRegistry(),
        context.getSession(),
        null,
        context.getSubstitutionProviderFactory(),
        context.getConfig(),
        context.getScanResult(),
        context.getRelMetadataQuerySupplier());
  }

  private static SqlNode parse(String sqlQueryText, QueryContext context) {
    SqlConverter parser = createConverter(context);
    return parser.parse(sqlQueryText);
  }

  private static SqlNode toQuery(SqlNode node) {
    // This is here since we might have some old UDFs that are not in a normalized format
    // But the main logic should happen now in CreateFunctionHandler
    final SqlKind kind = node.getKind();
    switch (kind) {
        // These are the node types that we know are already a query.
      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case WITH:
      case VALUES:
        return node;
      default:
        // We need to convert scalar values into a select statement
        return new SqlSelect(
            SqlParserPos.ZERO,
            null,
            new SqlNodeList(ImmutableList.of(node), SqlParserPos.ZERO),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }
  }
}
