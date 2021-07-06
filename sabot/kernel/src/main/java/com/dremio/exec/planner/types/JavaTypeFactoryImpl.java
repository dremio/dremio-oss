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
package com.dremio.exec.planner.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Note that this class extends from {@link org.apache.calcite.sql.type.SqlTypeFactoryImpl} indirectly, via
 * {@link org.apache.calcite.jdbc.JavaTypeFactoryImpl}, and not from {@link SqlTypeFactoryImpl} (in this package).
 */
public class JavaTypeFactoryImpl extends org.apache.calcite.jdbc.JavaTypeFactoryImpl {

  public static final JavaTypeFactoryImpl INSTANCE = new JavaTypeFactoryImpl();

  private JavaTypeFactoryImpl() {
    super(RelDataTypeSystemImpl.REL_DATA_TYPE_SYSTEM);
  }

  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision) {
    switch (typeName) {
    case CHAR:
      return super.createSqlType(SqlTypeName.VARCHAR, precision);
    case TIME:
    case TIMESTAMP:
      return super.createSqlType(typeName, RelDataTypeSystemImpl.SUPPORTED_DATETIME_PRECISION);
    default:
      return super.createSqlType(typeName, precision);
    }
  }

}
