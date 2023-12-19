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

import java.time.Instant;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;

/**
 * Base class that contains a reference (by type and value), an optional
 * requested timestamp, and an optional versioned source name.
 */
public abstract class SqlVersionSourceRefBase extends SqlVersionBase {

  private final ReferenceType refType;
  private final SqlIdentifier refValue;
  private final SqlNode timestamp;

  protected SqlVersionSourceRefBase(
      SqlParserPos pos,
      SqlIdentifier sourceName,
      ReferenceType refType,
      SqlIdentifier refValue,
      SqlNode timestamp) {
    super(pos, sourceName);
    this.refType = refType;
    this.refValue = refValue;
    this.timestamp = timestamp;
  }

  public ReferenceType getRefType() {
    return refType;
  }

  public SqlIdentifier getRefValue() {
    return refValue;
  }

  public Instant getTimestamp() {
    if (timestamp == null) {
      return null;
    }
    return Instant.ofEpochMilli(SqlHandlerUtil.convertToTimeInMillis(((SqlLiteral) timestamp).getValueAs(String.class), timestamp.getParserPosition()));
  }

  public SqlNode getTimestampAsSqlNode() { return timestamp; }

  public void unparseRef(SqlWriter writer, int leftPrec, int rightPrec, String prefix) {
    if (refType != null && refValue != null) {
      writer.keyword(prefix);
      writer.keyword(getRefType().toString());
      getRefValue().unparse(writer, leftPrec, rightPrec);

      if (timestamp != null) {
        writer.keyword("AS");
        writer.keyword("OF");
        writer.keyword(getTimestamp().toString());
      }
    }
  }
}
