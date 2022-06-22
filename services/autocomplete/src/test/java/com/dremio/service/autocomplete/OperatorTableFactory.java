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
package com.dremio.service.autocomplete;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Util;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.planner.sql.OperatorTable;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacro;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.tablefunctions.TableMacroNames;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Factory for OperatorTables
 */
public final class OperatorTableFactory {
  private static final SabotConfig SABOT_CONFIG = SabotConfig.create();
  private static final ScanResult SCAN_RESULT = ClassPathScanner.fromPrescan(SABOT_CONFIG);
  private static final FunctionImplementationRegistry FUNCTION_REGISTRY = new FunctionImplementationRegistry(
    SABOT_CONFIG,
    SCAN_RESULT);
  private static final List<String> TIME_TRAVEL_MACRO_NAME = TableMacroNames.TIME_TRAVEL;
  private static final List<String> TABLE_HISTORY_MACRO_NAME = ImmutableList.of("table_history");

  private static Map<List<String>, VersionedTableMacro> tableMacroMocks;

  public static SqlOperatorTable create() {
    return create(Collections.emptyList());
  }

  public static SqlOperatorTable create(List<SqlOperator> operators) {
    OperatorTable operatorTable = new OperatorTable(FUNCTION_REGISTRY);
    operatorTable.getOperatorList().removeAll(operatorTable.getOperatorList());
    for (SqlOperator sqlOperator : operators) {
      operatorTable.add(sqlOperator.getName(), sqlOperator);
    }

    return chainWithTimeTravel(operatorTable);
  }

  public static SqlOperatorTable createWithProductionFunctions(List<SqlOperator> operators) {
    FunctionImplementationRegistry functionRegistry = new FunctionImplementationRegistry(
      SABOT_CONFIG,
      SCAN_RESULT);
    OperatorTable operatorTable = new OperatorTable(functionRegistry);
    for (SqlOperator sqlOperator : operators) {
      operatorTable.add(sqlOperator.getName(), sqlOperator);
    }

    return chainWithTimeTravel(operatorTable);
  }

  private static SqlOperatorTable chainWithTimeTravel(SqlOperatorTable sqlOperatorTable) {
    // Mocks for table macro lookup
    initializeTableMacroMocks();
    final SqlOperatorTable timeTravelOperatorTable = mock(SqlOperatorTable.class);
    doAnswer(invocation -> {
      lookupOperatorOverloads(invocation.getArgument(0), invocation.getArgument(1),
        invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4));
      return null;
    }).when(timeTravelOperatorTable).lookupOperatorOverloads(any(), any(), any(), any(), any());

    return SqlOperatorTables.chain(sqlOperatorTable, timeTravelOperatorTable);
  }

  private static void initializeTableMacroMocks() {
    final List<FunctionParameter> functionParameters = new ReflectiveFunctionBase.ParameterListBuilder()
      .add(String.class, "table_name").build();
    final TranslatableTable table = mock(TranslatableTable.class);
    when(table.getRowType(any())).thenAnswer(invocation -> {
      RelDataTypeFactory typeFactory = invocation.getArgument(0);
      return typeFactory.createStructType(
        ImmutableList.of(typeFactory.createSqlType(SqlTypeName.INTEGER)), ImmutableList.of("id"));
    });

    tableMacroMocks = ImmutableMap.of(
      TIME_TRAVEL_MACRO_NAME, mock(VersionedTableMacro.class),
      TABLE_HISTORY_MACRO_NAME, mock(VersionedTableMacro.class)
    );

    tableMacroMocks.values().forEach(m -> {
      when(m.getParameters()).thenReturn(functionParameters);
      when(m.apply(any(), any())).thenReturn(table);
    });
  }

  private static void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category, SqlSyntax syntax,
                                              List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    if (category != SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION) {
      return;
    }

    VersionedTableMacro tableMacro = tableMacroMocks.get(opName.names);
    if (tableMacro != null) {
      List<RelDataType> argTypes = new ArrayList<>();
      List<SqlTypeFamily> typeFamilies = new ArrayList<>();
      for (FunctionParameter o : tableMacro.getParameters()) {
        final RelDataType type = o.getType(JavaTypeFactoryImpl.INSTANCE);
        argTypes.add(type);
        typeFamilies.add(
          Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
      }
      final IntPredicate isParameterAtIndexOptional = index ->
        tableMacro.getParameters().get(index).isOptional();
      final FamilyOperandTypeChecker typeChecker =
        OperandTypes.family(typeFamilies, isParameterAtIndexOptional::test);
      final List<RelDataType> paramTypes = toSql(argTypes);
      SqlVersionedTableMacro operator = new SqlVersionedTableMacro(opName, ReturnTypes.CURSOR,
        InferTypes.explicit(argTypes), typeChecker, paramTypes, tableMacro);

      operatorList.add(operator);
    }
  }

  private static List<RelDataType> toSql(List<RelDataType> types) {
    return types.stream().map(OperatorTableFactory::toSql).collect(Collectors.toList());
  }

  private static RelDataType toSql(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
      && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
      == Object.class) {
      return JavaTypeFactoryImpl.INSTANCE.createTypeWithNullability(
        JavaTypeFactoryImpl.INSTANCE.createSqlType(SqlTypeName.ANY), true);
    }
    return JavaTypeFactoryImpl.INSTANCE.toSql(type);
  }
}
