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
import java.util.UUID;

import com.google.common.base.Preconditions;

/**
 * Type for a column and it's parent table alias.
 */
public final class ColumnAndTableAlias {
  private final Column column;
  private final String tableAlias;

  private ColumnAndTableAlias(Column column, String tableAlias) {
    Preconditions.checkNotNull(column);
    Preconditions.checkNotNull(tableAlias);

    this.column = column;
    this.tableAlias = tableAlias;
  }

  public static ColumnAndTableAlias createWithTable(Column column, String tableAlias) {
    return new ColumnAndTableAlias(column, tableAlias);
  }

  // Current version of the column resolver needs to be improved and this should go away.
  // The only place that this is called is when we are inferring columns that are available in subselect
  // and table alias doesn't matter in this case.
  public static ColumnAndTableAlias createAnonymous(Column column) {
    return new ColumnAndTableAlias(column, UUID.randomUUID().toString());
  }

  public Column getColumn() {
    return column;
  }

  public String getTableAlias() {
    return tableAlias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnAndTableAlias that = (ColumnAndTableAlias) o;
    return column.equals(that.column) && tableAlias.equals(that.tableAlias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, tableAlias);
  }
}
