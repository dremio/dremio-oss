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

/**
 * Extends SqlDataTypeSpec to support complex specs.
 * Facilitates customized deriveType for complex types
 */
public class SqlComplexDataTypeSpec extends SqlDataTypeSpec {

  public SqlComplexDataTypeSpec(SqlDataTypeSpec spec) {
    super(spec.getCollectionsTypeName(),
      spec.getTypeName(),
      spec.getTypeName(),
      spec.getPrecision(),
      spec.getScale(),
      spec.getCharSetName(),
      spec.getTimeZone(),
      spec.getNullable(),
      spec.getParserPosition());
  }

  @Override
  public RelDataType deriveType(RelDataTypeFactory typeFactory) {
    if (this.getTypeName() instanceof SqlTypeNameSpec) {
      // Create type directly if this typeName is a SqlTypeNameSpec.
      return getDataTypeForComplex(typeFactory);
    }
    return super.deriveType(typeFactory); // DEFAULT
  }

  private RelDataType getDataTypeForComplex(RelDataTypeFactory typeFactory) {
    RelDataType type = createTypeFromTypeNameSpec(typeFactory, (SqlTypeNameSpec) this.getTypeName());
    if (type == null) {
      return null;
    }
    type = typeFactory.createTypeWithNullability(type, this.getNullable() == null ? true : this.getNullable());

    return type;
  }

  /**
   * Create type from the type name specification directly.
   *
   * @param typeFactory type factory.
   * @return the type.
   */
  private RelDataType createTypeFromTypeNameSpec(
    RelDataTypeFactory typeFactory,
    SqlTypeNameSpec typeNameSpec) {
    return typeNameSpec.deriveType(typeFactory);
  }
}
