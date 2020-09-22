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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.service.namespace.NamespaceKey;

/**
 * Abstract class for 'ALTER TABLE' - unparses 'ALTER TABLE' part of the command
 */
public abstract class SqlAlterTable extends SqlCall {

  protected final SqlIdentifier tblName;

  public SqlAlterTable(SqlParserPos pos, SqlIdentifier tblName) {
    super(pos);
    this.tblName = tblName;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TABLE");
    tblName.unparse(writer, leftPrec, rightPrec);
  }

  public NamespaceKey getTable() {
    return new NamespaceKey(tblName.names);
  }
}
