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

import com.dremio.exec.planner.sql.PartitionTransform;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Parse tree node for a ALTER TABLE ... ADD|DROP PARTITION FIELD statement. */
public class SqlAlterTablePartitionColumns extends SqlAlterTable {

  public enum Mode {
    ADD,
    DROP
  }

  public static final SqlSpecialOperator ALTER_PARTITION_COLUMNS_OPERATOR =
      new SqlSpecialOperator("ALTER_PARTITION_COLUMNS", SqlKind.ALTER_TABLE) {

        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          Preconditions.checkArgument(operands.length == 4);

          return new SqlAlterTablePartitionColumns(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              (SqlPartitionTransform) operands[2],
              (SqlTableVersionSpec) operands[3]);
        }
      };

  private final SqlLiteral mode;
  private final SqlPartitionTransform partitionTransform;
  private final SqlTableVersionSpec tableVersionSpec;

  public SqlAlterTablePartitionColumns(
      SqlParserPos pos,
      SqlIdentifier tableName,
      SqlLiteral mode,
      SqlPartitionTransform partitionTransform,
      SqlTableVersionSpec tableVersionSpec) {
    super(pos, tableName);
    this.mode = Preconditions.checkNotNull(mode);
    this.partitionTransform = Preconditions.checkNotNull(partitionTransform);
    this.tableVersionSpec = Preconditions.checkNotNull(tableVersionSpec);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    writer.keyword(mode.toValue());
    writer.keyword("PARTITION");
    writer.keyword("FIELD");
    partitionTransform.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  public SqlOperator getOperator() {
    return ALTER_PARTITION_COLUMNS_OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Lists.newArrayList(tblName, mode, partitionTransform, tableVersionSpec);
  }

  public Mode getMode() {
    return mode.symbolValue(Mode.class);
  }

  public PartitionTransform getPartitionTransform() {
    return PartitionTransform.from(partitionTransform);
  }

  public SqlTableVersionSpec getSqlTableVersionSpec() {
    return tableVersionSpec;
  }
}
