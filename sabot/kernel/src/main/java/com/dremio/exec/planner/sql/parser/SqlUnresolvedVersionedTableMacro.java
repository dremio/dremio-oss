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


import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.catalog.model.dataset.TableVersionType;

/**
 * Implementation of {@link SqlUnresolvedFunction} which represents an unresolved versioned table macro used with
 * time travel. Version information, in the form of a {@link TableVersionSpec}, is maintained locally.  This version
 * information is transferred to a {@link SqlVersionedTableMacro} once table macro resolution happens during
 * validation.
 */
public class SqlUnresolvedVersionedTableMacro extends SqlUnresolvedFunction implements HasTableVersion {

  private TableVersionSpec tableVersionSpec;

  public SqlUnresolvedVersionedTableMacro(
    SqlIdentifier sqlIdentifier,
    TableVersionType tableVersionType,
    SqlNode versionSpecifier,
    SqlNode timestamp) {
    super(sqlIdentifier, null, null, null, null, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION);
    this.tableVersionSpec = new TableVersionSpec(tableVersionType, versionSpecifier, timestamp);
  }

  @Override
  public TableVersionSpec getTableVersionSpec() {
    return tableVersionSpec;
  }

  @Override
  public void setTableVersionSpec(TableVersionSpec tableVersionSpec) {
    this.tableVersionSpec = tableVersionSpec;
  }

  @Override
  public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
    return new SqlVersionedTableMacroCall(this, operands, pos);
  }
}
