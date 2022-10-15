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
package com.dremio.service.autocomplete.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.DremioEmptyScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.exec.catalog.DremioCatalogReader;
import com.dremio.exec.catalog.SimpleCatalog;
import com.dremio.exec.planner.sql.DremioSqlConformance;
import com.dremio.exec.planner.sql.SqlValidatorImpl;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.google.common.base.Preconditions;

/**
 * Factory for SqlValidator and Scope.
 */
public final class SqlValidatorAndScopeFactory {
  public static Result create(SqlOperatorTable operatorTable, SimpleCatalog<?> catalog) {
    Preconditions.checkNotNull(operatorTable);
    Preconditions.checkNotNull(catalog);

    final DremioCatalogReader catalogReader = new DremioCatalogReader(
      catalog,
      JavaTypeFactoryImpl.INSTANCE);
    final SqlOperatorTable chainedOperatorTable = ChainedSqlOperatorTable.of(
      operatorTable,
      catalogReader);
    final SqlValidatorImpl validator = new MockSqlValidator(
      new SqlValidatorImpl.FlattenOpCounter(),
      chainedOperatorTable,
      catalogReader,
      JavaTypeFactoryImpl.INSTANCE,
      DremioSqlConformance.INSTANCE,
      null);
    final SqlValidatorScope scope = new MockScope(validator);

    return new Result(validator, scope);
  }

  private static final class MockSqlValidator extends com.dremio.exec.planner.sql.SqlValidatorImpl {

    public MockSqlValidator(FlattenOpCounter flattenCount, SqlOperatorTable sqlOperatorTable, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlConformance conformance, OptionResolver optionResolver) {
      super(flattenCount, sqlOperatorTable, catalogReader, typeFactory, conformance, optionResolver);
    }

    @Override
    public Config config() {
      return super.config().withTypeCoercionEnabled(false);
    }
  }

  public static final class Result {
    private final SqlValidator sqlValidator;
    private final SqlValidatorScope scope;

    public Result(SqlValidator sqlValidator, SqlValidatorScope scope) {
      Preconditions.checkNotNull(sqlValidator);
      Preconditions.checkNotNull(scope);

      this.sqlValidator = sqlValidator;
      this.scope = scope;
    }

    public SqlValidator getSqlValidator() {
      return sqlValidator;
    }

    public SqlValidatorScope getScope() {
      return scope;
    }
  }

  private static final class MockScope extends DremioEmptyScope {
    public MockScope(SqlValidatorImpl validator) {
      super(validator);
    }

    @Override
    public RelDataType resolveColumn(String name, SqlNode ctx) {
      SqlIdentifier identifier = (SqlIdentifier) ctx;
      DremioCatalogReader dremioCatalogReader = (DremioCatalogReader) this.validator.getCatalogReader();
      RelDataType tableSchema = dremioCatalogReader
        .getTableSchema(identifier.names.subList(0, identifier.names.size() - 1))
        .get();

      return tableSchema;
    }
  }
}
