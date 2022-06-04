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
package com.dremio.service.autocomplete.columns;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;

/**
 * Column datatype.
 */
public final class Column {
  private final String name;
  private final RelDataType type;

  public Column(String name, RelDataType type) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);

    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public RelDataType getType() {
    return type;
  }

  public static Column create(String name, SqlTypeName sqlTypeName) {
    return new Column(name, JavaTypeFactoryImpl.INSTANCE.createSqlType(sqlTypeName));
  }
}
