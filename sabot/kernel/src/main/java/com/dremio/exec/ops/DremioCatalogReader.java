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
package com.dremio.exec.ops;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.IntPredicate;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMoniker;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Util;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.catalog.DremioSchema;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.DremioTranslatableTable;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.parser.SqlVersionedTableMacro;
import com.dremio.exec.tablefunctions.VersionedTableMacro;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Dremio implementation of several interfaces that are typically provided by CalciteCatalogReader.
 * Interacts with {@link PlannerCatalog} object to validate and return tables.
 */
public class DremioCatalogReader implements SqlValidatorCatalogReader, Prepare.CatalogReader, SqlOperatorTable {

  private final PlannerCatalog plannerCatalog;

  private final List<List<String>> schemaPaths;

  public DremioCatalogReader(PlannerCatalog plannerCatalog) {
    this.plannerCatalog = plannerCatalog;

    ImmutableList.Builder<List<String>> schemaPaths = ImmutableList.builder();
    if (plannerCatalog.getDefaultSchema() != null && !plannerCatalog.getDefaultSchema().getPathComponents().isEmpty()) {
      // If not empty, this is the schema set by the UI
      schemaPaths.add(ImmutableList.copyOf(plannerCatalog.getDefaultSchema().getPathComponents()));
    }
    // No schema to support fully qualified table names in SQL
    schemaPaths.add(ImmutableList.of());
    this.schemaPaths = schemaPaths.build();
  }

  @WithSpan("catalog-get-table")
  @Override
  public DremioPrepareTable getTable(List<String> paramList) {
    NamespaceKey namespaceKey = new NamespaceKey(paramList);
    Span.current().setAttribute("dremio.table.name", namespaceKey.getSchemaPath());
    final DremioTable table = plannerCatalog.getValidatedTableWithSchema(namespaceKey);
    if(table == null) {
      return null;
    }
    return new DremioPrepareTable(this, plannerCatalog.getTypeFactory(), table);
  }

  /**
   * Gets the DremioTable(does not resolve to schema path)
   * @param catalogEntityKey expects a fully resolved CatalogEntityKey path
   * @return
   */
  public DremioTable getTable(CatalogEntityKey catalogEntityKey) {
    return plannerCatalog.getValidatedTableSnapshotWithSchema(catalogEntityKey.toNamespaceKey(), catalogEntityKey.getTableVersionContext());
  }

  public DremioTranslatableTable getTableSnapshot(NamespaceKey key, TableVersionContext context) {
    return plannerCatalog.getTableSnapshotIgnoreSchema(key, context);
  }

  /**
   * Used to get the table schema
   *
   * @param paramList
   * @return
   */
  public Optional<RelDataType> getTableSchema(List<String> paramList) {
    final DremioTable table = plannerCatalog.getTableWithSchema(new NamespaceKey(paramList));
    if(table == null) {
      return Optional.empty();
    }
    return Optional.of(new DremioPrepareTable(this, plannerCatalog.getTypeFactory(), table).getRowType());
  }

  public void validateSelection() {
    plannerCatalog.validateSelection();
  }

  @Override
  public RelDataType getNamedType(SqlIdentifier paramSqlIdentifier) {
    return null;
  }

  @Override
  public List<SqlMoniker> getAllSchemaObjectNames(List<String> paramList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<List<String>> getSchemaPaths() {
    return schemaPaths;
  }

  @Override
  public RelDataTypeField field(RelDataType rowType, String columnName) {
    return rowType.getField(columnName, false, false);
  }

  @Override
  public boolean matches(String paramString1, String paramString2) {
    return paramString1.equalsIgnoreCase(paramString2);
  }

  @Override
  public RelDataType createTypeFromProjection(RelDataType paramRelDataType, List<String> paramList) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCaseSensitive() {
    return false;
  }

  @Override
  public CalciteSchema getRootSchema() {
    return new DremioSchema(plannerCatalog, new NamespaceKey(ImmutableList.of()));
  }

  @Override
  public CalciteConnectionConfig getConfig() {
    throw new UnsupportedOperationException("Calcite Catalog DX15967");
  }

  @Override
  public SqlNameMatcher nameMatcher() {
    return SqlNameMatchers.withCaseSensitive(false);
  }

  @Override
  public RelDataTypeFactory getTypeFactory() {
    return plannerCatalog.getTypeFactory();
  }

  @Override
  public void registerRules(RelOptPlanner paramRelOptPlanner) throws Exception {
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return ImmutableList.of();
  }

  @Override
  public DremioPrepareTable getTableForMember(List<String> paramList) {
    final DremioTable table = plannerCatalog.getValidatedTableIgnoreSchema(new NamespaceKey(paramList));
    if(table == null) {
      return null;
    }
    return new DremioPrepareTable(this, plannerCatalog.getTypeFactory(), table);
  }

  @Override
  public DremioCatalogReader withSchemaPath(List<String> newNamespacePath) {
    NamespaceKey withSchemaPath = newNamespacePath == null ? null : new NamespaceKey(newNamespacePath);
    return new DremioCatalogReader(plannerCatalog.resolvePlannerCatalog(withSchemaPath));
  }

  @Override
  public void lookupOperatorOverloads(final SqlIdentifier paramSqlIdentifier,
                                      SqlFunctionCategory paramSqlFunctionCategory,
                                      SqlSyntax paramSqlSyntax,
                                      List<SqlOperator> paramList,
                                      SqlNameMatcher nameMatcher) {
    if(null == paramSqlFunctionCategory
        || null == paramSqlIdentifier) {
      return;
    }
    findFunctions(new NamespaceKey(paramSqlIdentifier.names), paramSqlFunctionCategory).stream()
      .map(input -> toOp(paramSqlIdentifier, input))
      .forEach(paramList::add);

  }

  private Collection<Function> findFunctions(
    NamespaceKey namespaceKey,
    SqlFunctionCategory paramSqlFunctionCategory) {
    switch (paramSqlFunctionCategory) {
      case USER_DEFINED_FUNCTION:
        return plannerCatalog.getFunctions(namespaceKey, SimpleCatalog.FunctionType.SCALAR);
      case USER_DEFINED_TABLE_FUNCTION:
        return plannerCatalog.getFunctions(namespaceKey, SimpleCatalog.FunctionType.TABLE);
      default:
        return ImmutableList.of();
    }
  }

  /**
   * Based on:
   * org.apache.calcite.prepare.CalciteCatalogReader.toOp(SqlIdentifier name, final Function function)
   * org.apache.calcite.prepare.CalciteCatalogReader.toOp(RelDataTypeFactory typeFactory, SqlIdentifier name, final Function function)
   * This is because that class considers these utilities to be private concerns.
   */
  private SqlOperator toOp(SqlIdentifier name, final Function function) {
    List<RelDataType> argTypes = new ArrayList<>();
    List<SqlTypeFamily> typeFamilies = new ArrayList<>();
    for (FunctionParameter o : function.getParameters()) {
      final RelDataType type = o.getType(plannerCatalog.getTypeFactory());
      argTypes.add(type);
      typeFamilies.add(
          Util.first(type.getSqlTypeName().getFamily(), SqlTypeFamily.ANY));
    }
    final IntPredicate isParameterAtIndexOptional = index ->
      function.getParameters().get(index).isOptional();
    final FamilyOperandTypeChecker typeChecker =
        OperandTypes.family(typeFamilies, isParameterAtIndexOptional::test);
    final List<RelDataType> paramTypes = toSql(argTypes);
    if (function instanceof ScalarFunction) {
      return new SqlUserDefinedFunction(name, infer((ScalarFunction) function),
          InferTypes.explicit(argTypes), typeChecker, paramTypes, function);
    } else if (function instanceof AggregateFunction) {
      return new SqlUserDefinedAggFunction(name,
          infer((AggregateFunction) function), InferTypes.explicit(argTypes),
          typeChecker, (AggregateFunction) function, false, false, Optionality.FORBIDDEN, plannerCatalog.getTypeFactory());
    } else if (function instanceof VersionedTableMacro) {
      return new SqlVersionedTableMacro(name, ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), typeChecker, paramTypes,
          (VersionedTableMacro) function);
    } else if (function instanceof TableMacro) {
      return new SqlUserDefinedTableMacro(name, ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), typeChecker, paramTypes,
          (TableMacro) function);
    } else if (function instanceof TableFunction) {
      return new SqlUserDefinedTableFunction(name, ReturnTypes.CURSOR,
          InferTypes.explicit(argTypes), typeChecker, paramTypes,
          (TableFunction) function);
    } else {
      throw new AssertionError("unknown function type " + function);
    }
  }

  /**
   * Based on:
   * org.apache.calcite.prepare.CalciteCatalogReader.infer(final ScalarFunction function)
   * This is because that class considers these utilities to be private concerns.
   */
  private SqlReturnTypeInference infer(final ScalarFunction function) {
    return new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        final RelDataType type = function.getReturnType(plannerCatalog.getTypeFactory());
        return toSql(type);
      }
    };
  }

  /**
   * Based on:
   * org.apache.calcite.prepare.CalciteCatalogReader.infer(final AggregateFunction function)
   * This is because that class considers these utilities to be private concerns.
   */
  private SqlReturnTypeInference infer(final AggregateFunction function) {
    return new SqlReturnTypeInference() {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        final RelDataType type = function.getReturnType(plannerCatalog.getTypeFactory());
        return toSql(type);
      }
    };
  }

  /**
   * Based on:
   * org.apache.calcite.prepare.CalciteCatalogReader.toSql(final RelDataTypeFactory typeFactory, List<RelDataType> types)
   * This is because that class considers these utilities to be private concerns.
   */
  private List<RelDataType> toSql(List<RelDataType> types) {
    return Lists.transform(types,
        new com.google.common.base.Function<RelDataType, RelDataType>() {
          @Override
          public RelDataType apply(RelDataType type) {
            return toSql(type);
          }
        });
  }

  /**
   * Based on:
   * org.apache.calcite.prepare.CalciteCatalogReader.toSql(RelDataTypeFactory typeFactory, RelDataType type)
   * This is because that class considers these utilities to be private concerns.
   */
  private RelDataType toSql(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType
        && ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass()
        == Object.class) {
      return plannerCatalog.getTypeFactory().createTypeWithNullability(
        plannerCatalog.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
    }
    return plannerCatalog.getTypeFactory().toSql(type);
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return null;
  }
}
