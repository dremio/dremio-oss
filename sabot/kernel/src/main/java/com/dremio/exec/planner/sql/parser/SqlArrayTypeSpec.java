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
package com.dremio.exec.planner.sql.parser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Supports list<bigint> type of array schema
 *
 * <p>We also support to add a [ NULL | NOT NULL ] suffix for every field type
 */
public final class SqlArrayTypeSpec extends SqlTypeNameSpec implements ValidatableTypeNameSpec {

  private final SqlComplexDataTypeSpec spec;

  public SqlArrayTypeSpec(SqlParserPos pos, SqlComplexDataTypeSpec spec) {
    super(SqlTypeName.ARRAY.name(), pos);
    this.spec = spec;
  }

  @Override
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    RelDataType type = spec.deriveType(new DremioSqlValidator(typeFactory));
    return typeFactory.createArrayType(type, -1);
  }

  @Override
  public RelDataType deriveType(SqlValidator sqlValidator) {
    RelDataType type = spec.deriveType(sqlValidator);
    return sqlValidator.getTypeFactory().createArrayType(type, -1);
  }

  /**
   * Validate the ARRAY type, it cannot have incorrect basic type.
   *
   * @return the invalid SqlDataTypeSpec or null when the type is valid.
   */
  @Override
  public SqlDataTypeSpec validateType() {
    if (SqlTypeName.get(spec.getTypeName().getSimple()) == null) {
      return spec;
    }
    return spec.validateType();
  }

  public SqlComplexDataTypeSpec getSpec() {
    return spec;
  }
}
