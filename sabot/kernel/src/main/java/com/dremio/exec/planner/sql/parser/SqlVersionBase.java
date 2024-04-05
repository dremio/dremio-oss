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

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.sql.handlers.direct.SqlDirectHandler;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Base class that contains an optional versioned source name. */
public abstract class SqlVersionBase extends SqlCall {

  private final SqlIdentifier sourceName;

  protected SqlVersionBase(SqlParserPos pos, SqlIdentifier sourceName) {
    super(pos);
    this.sourceName = sourceName;
  }

  public abstract SqlDirectHandler<?> toDirectHandler(QueryContext context);

  public SqlIdentifier getSourceName() {
    return sourceName;
  }

  public void unparseSourceName(SqlWriter writer, int leftPrec, int rightPrec) {
    if (getSourceName() != null) {
      writer.keyword("IN");
      getSourceName().unparse(writer, leftPrec, rightPrec);
    }
  }
}
