/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.ischema;

import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Table;

import com.dremio.exec.planner.logical.ViewInfoProvider;
import com.google.common.base.Preconditions;

/**
 * Minimal table info needed for INFORMATION_SCHEMA.{`TABLES`, `VIEWS`} record generation.
 */
public class TableInfo {
  private final TableType type;
  private final Table table;

  public TableInfo(TableType type) {
    this.type = type;
    this.table = null;
  }

  public TableInfo(Table table) {
    this.type = table.getJdbcTableType();
    this.table = table;
  }

  /**
   * Get table type (VIEW or TABLE etc).
   * @return
   */
  public TableType getJdbcTableType() {
    return type;
  }

  /**
   * Get the view definition. If fully materialized table is not available or the table is not a view, an
   * exception is thrown.
   *
   * @return
   */
  public String getViewSQL() {
    Preconditions.checkNotNull(table, "Fully materialized table is not available.");
    Preconditions.checkState(table.getJdbcTableType() == TableType.VIEW, "Table is not a view.");

    return ((ViewInfoProvider)table).getViewSql();
  }

  /**
   * Get the underlying fully materialized table instance. May return null if one doesn't exists.
   *
   * @return Fully materialized table if exists, otherwise null
   */
  public Table getTable() {
    return table;
  }
}
