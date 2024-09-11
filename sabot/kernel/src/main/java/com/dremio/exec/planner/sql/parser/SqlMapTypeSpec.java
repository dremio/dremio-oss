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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Supports map<bigint, bigint> type of map schema where the key cannot be complex type.
 *
 * <p>We also support to add a [ NULL | NOT NULL ] suffix for every field type of the value.
 */
public final class SqlMapTypeSpec extends SqlTypeNameSpec implements ValidatableTypeNameSpec {

  private final SqlComplexDataTypeSpec specKey;
  private final SqlComplexDataTypeSpec specValue;

  public SqlMapTypeSpec(
      SqlParserPos pos, SqlComplexDataTypeSpec specKey, SqlComplexDataTypeSpec specValue) {
    super(SqlTypeName.MAP.name(), pos);
    this.specKey = specKey;
    this.specValue = specValue;
  }

  @Override
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    return typeFactory.createMapType(
        specKey.getDataTypeForComplex(typeFactory), specValue.getDataTypeForComplex(typeFactory));
  }

  @Override
  public RelDataType deriveType(SqlValidator sqlValidator) {
    return sqlValidator
        .getTypeFactory()
        .createMapType(specKey.deriveType(sqlValidator), specValue.deriveType(sqlValidator));
  }

  /**
   * Validate the MAP type, it cannot have incorrect basic types, the key must be a basic type.
   *
   * @return the invalid SqlDataTypeSpec or null when the type is valid.
   */
  @Override
  public SqlDataTypeSpec validateType() {
    if (SqlTypeName.get(specKey.getTypeName().getSimple()) == null) {
      return specKey;
    }
    if (SqlTypeName.get(specValue.getTypeName().getSimple()) == null) {
      return specValue;
    }
    if (!(specKey.getTypeNameSpec() instanceof SqlBasicTypeNameSpec)) {
      return specKey;
    }
    return specValue.validateType();
  }

  public SqlComplexDataTypeSpec getSpecKey() {
    return specKey;
  }

  public SqlComplexDataTypeSpec getSpecValue() {
    return specValue;
  }
}
