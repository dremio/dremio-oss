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

import java.util.Objects;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Column datatype.
 */
public final class Column {
  private final String name;
  private final SqlTypeName type;

  private Column(String name, SqlTypeName type) {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);

    this.name = name;
    this.type = type;
  }

  public static Column typedColumn(String name, SqlTypeName type) {
    return new Column(name, type);
  }

  public static Column anyTypeColumn(String name) {
    return new Column(name, SqlTypeName.ANY);
  }

  public String getName() {
    return name;
  }

  public SqlTypeName getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Column column = (Column) o;
    return name.equals(column.name) && type == column.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return "Column{" +
      "name='" + name + '\'' +
      ", type=" + type +
      '}';
  }
}
