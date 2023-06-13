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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Defines a column in a tabular function return spec.
 */
public final class SqlReturnField extends SqlNodeList {
  private SqlIdentifier name;
  private SqlComplexDataTypeSpec spec;

  public SqlReturnField(SqlParserPos pos, SqlIdentifier name, SqlComplexDataTypeSpec spec) {
    super(pos);
    this.name = name;
    this.spec = spec;
  }

  public SqlIdentifier getName() {
    return name;
  }

  public SqlComplexDataTypeSpec getType() {
    return spec;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    spec.unparse(writer, leftPrec, rightPrec);
  }
}
