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

import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/** SQL node tree for <code>ALTER TABLE table_identifier REFRESH METADATA</code> */
public class SqlRefreshTable extends SqlSystemCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REFRESH_TABLE", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlRefreshTable(
              pos,
              (SqlIdentifier) operands[0],
              (SqlLiteral) operands[1],
              (SqlLiteral) operands[2],
              (SqlLiteral) operands[3],
              (SqlLiteral) operands[4],
              (SqlLiteral) operands[5],
              (SqlLiteral) operands[6],
              (SqlLiteral) operands[7],
              (SqlNodeList) operands[8],
              (SqlNodeList) operands[9]);
        }
      };

  private SqlIdentifier table;
  private SqlLiteral deleteUnavail;
  private SqlLiteral forceUp;
  private SqlLiteral promotion;
  private SqlLiteral allFilesRefresh;
  private SqlLiteral fileRefresh;
  private SqlLiteral allPartitionsRefresh;
  private SqlLiteral partitionRefresh;
  private SqlNodeList filesList;
  private SqlNodeList partitionList;

  /** Creates a SqlForgetTable. */
  public SqlRefreshTable(
      SqlParserPos pos,
      SqlIdentifier table,
      SqlLiteral deleteUnavail,
      SqlLiteral forceUp,
      SqlLiteral promotion,
      SqlLiteral allFilesRefresh,
      SqlLiteral allPartitionsRefresh,
      SqlLiteral fileRefresh,
      SqlLiteral partitionRefresh,
      SqlNodeList filesList,
      SqlNodeList partitionList) {
    super(pos);
    this.table = table;
    this.deleteUnavail = deleteUnavail;
    this.forceUp = forceUp;
    this.promotion = promotion;
    this.allFilesRefresh = allFilesRefresh;
    this.fileRefresh = fileRefresh;
    this.allPartitionsRefresh = allPartitionsRefresh;
    this.partitionRefresh = partitionRefresh;
    this.filesList = filesList;
    this.partitionList = partitionList;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);
    writer.keyword("REFRESH");
    writer.keyword("METADATA");

    if (allFilesRefresh.getValue() != null) {
      if (allFilesRefresh.booleanValue()) {
        writer.keyword("FOR");
        writer.keyword("ALL");
        writer.keyword("FILES");
      }
    }

    if (allPartitionsRefresh.getValue() != null) {
      if (allPartitionsRefresh.booleanValue()) {
        writer.keyword("FOR");
        writer.keyword("ALL");
        writer.keyword("PARTITIONS");
      }
    }

    if (fileRefresh.getValue() != null) {
      if (fileRefresh.booleanValue()) {
        writer.keyword("FOR");
        writer.keyword("FILES");
        if (filesList.size() > 0) {
          SqlHandlerUtil.unparseSqlNodeList(writer, leftPrec, rightPrec, filesList);
        }
      }
    }

    if (partitionRefresh.getValue() != null) {
      if (partitionRefresh.booleanValue()) {
        writer.keyword("FOR");
        writer.keyword("PARTITIONS");
        if (partitionList.size() > 0) {
          writer.keyword("(");
          partitionList.forEach(
              node -> {
                final SqlNodeList pair = (SqlNodeList) node;
                pair.get(0).unparse(writer, leftPrec, rightPrec);
                writer.keyword("=");
                pair.get(1).unparse(writer, leftPrec, rightPrec);
              });
          writer.keyword(")");
        }
      }
    }

    if (deleteUnavail.getValue() != null) {
      if (deleteUnavail.booleanValue()) {
        writer.keyword("DELETE");
      } else {
        writer.keyword("MAINTAIN");
      }
      writer.keyword("WHEN");
      writer.keyword("MISSING");
    }

    if (forceUp.getValue() != null) {
      if (forceUp.booleanValue()) {
        writer.keyword("FORCE");
      } else {
        writer.keyword("LAZY");
      }
      writer.keyword("UPDATE");
    }

    if (promotion.getValue() != null) {
      if (promotion.booleanValue()) {
        writer.keyword("AUTO");
      } else {
        writer.keyword("AVOID");
      }
      writer.keyword("PROMOTION");
    }
  }

  @Override
  public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        table = (SqlIdentifier) operand;
        break;
      case 1:
        deleteUnavail = (SqlLiteral) operand;
        break;
      case 2:
        forceUp = (SqlLiteral) operand;
        break;
      case 3:
        promotion = (SqlLiteral) operand;
        break;
      case 4:
        allFilesRefresh = (SqlLiteral) operand;
        break;
      case 5:
        allPartitionsRefresh = (SqlLiteral) operand;
        break;
      case 6:
        fileRefresh = (SqlLiteral) operand;
        break;
      case 7:
        partitionRefresh = (SqlLiteral) operand;
        break;
      case 8:
        filesList = (SqlNodeList) operand;
        break;
      case 9:
        partitionList = (SqlNodeList) operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.<SqlNode>of(
        table,
        deleteUnavail,
        forceUp,
        promotion,
        allFilesRefresh,
        allPartitionsRefresh,
        fileRefresh,
        partitionRefresh,
        filesList,
        partitionList);
  }

  public SqlIdentifier getTable() {
    return table;
  }

  public SqlLiteral getDeleteUnavail() {
    return deleteUnavail;
  }

  public SqlLiteral getForceUpdate() {
    return forceUp;
  }

  public SqlLiteral getPromotion() {
    return promotion;
  }

  public SqlLiteral getAllFilesRefresh() {
    return allFilesRefresh;
  }

  public SqlLiteral getFileRefresh() {
    return fileRefresh;
  }

  public SqlNodeList getFilesList() {
    return filesList;
  }

  public SqlNodeList getPartitionList() {
    return partitionList;
  }

  public List<String> getFileNames() {
    List<String> fileNames =
        filesList.getList().stream()
            .map(o -> ((SqlLiteral) o).getValueAs(String.class))
            .collect(Collectors.toList());
    return fileNames;
  }

  public SqlLiteral getAllPartitionsRefresh() {
    return allPartitionsRefresh;
  }

  public SqlLiteral getPartitionRefresh() {
    return partitionRefresh;
  }

  public String toRefreshDatasetQuery(
      List<String> pathComponents,
      Optional<String> fileNameRegex,
      boolean errorOnConcurrentRefresh) {
    return new SqlRefreshDataset(
            pos,
            new SqlIdentifier(pathComponents, pos),
            deleteUnavail,
            forceUp,
            promotion,
            allFilesRefresh,
            allPartitionsRefresh,
            fileRefresh,
            partitionRefresh,
            filesList,
            partitionList,
            fileNameRegex.map(p -> SqlLiteral.createCharString(p, SqlParserPos.ZERO)).orElse(null),
            SqlLiteral.createBoolean(errorOnConcurrentRefresh, SqlParserPos.ZERO))
        .toSqlString(DremioSqlDialect.CALCITE)
        .getSql();
  }
}
