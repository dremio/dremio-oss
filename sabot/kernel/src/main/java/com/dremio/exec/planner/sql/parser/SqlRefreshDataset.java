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

import com.dremio.exec.planner.sql.handlers.RefreshDatasetHandler;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;

/** SQL node tree for the internal <code>REFRESH DATASET table_identifier</code> command. */
public class SqlRefreshDataset extends SqlCall implements SqlToPlanHandler.Creator {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("REFRESH_DATASET", SqlKind.OTHER) {
        @Override
        public SqlCall createCall(
            SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
          return new SqlRefreshDataset(
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
              (SqlNodeList) operands[9],
              operands[10],
              (SqlLiteral) operands[11]);
        }
      };
  private SqlIdentifier table;
  private SqlLiteral deleteUnavail;
  private SqlLiteral forceUp;
  private SqlLiteral promotion;
  private SqlLiteral allFilesRefresh;
  private SqlLiteral allPartitionsRefresh;
  private SqlLiteral fileRefresh;
  private SqlLiteral partitionRefresh;
  private SqlNodeList filesList;
  private SqlNodeList partitionList;
  private SqlNode fileNameRegex;
  private SqlLiteral errorOnConcurrentRefresh;
  private Map<String, String> partitionKVMap;

  /** Creates a SqlRefreshDataset. */
  public SqlRefreshDataset(
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
      SqlNodeList partitionList,
      SqlNode fileNameRegex,
      SqlLiteral errorOnConcurrentRefresh) {
    super(pos);
    this.table = table;
    this.deleteUnavail = deleteUnavail;
    this.forceUp = forceUp;
    this.promotion = promotion;
    this.allFilesRefresh = allFilesRefresh;
    this.allPartitionsRefresh = allPartitionsRefresh;
    this.fileRefresh = fileRefresh;
    this.partitionRefresh = partitionRefresh;
    this.filesList = filesList;
    this.partitionList = partitionList;
    this.fileNameRegex = fileNameRegex;
    this.errorOnConcurrentRefresh = errorOnConcurrentRefresh;
    setPartitionKVMap();
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    writer.keyword("DATASET");
    getTable().unparse(writer, leftPrec, rightPrec);
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
          unparseKeyValuePair((SqlNodeList) partitionList.get(0), writer, leftPrec, rightPrec);
          for (int i = 1; i < partitionList.size(); i++) {
            writer.keyword(",");
            unparseKeyValuePair((SqlNodeList) partitionList.get(i), writer, leftPrec, rightPrec);
          }
          writer.keyword(")");
        }
      }
    }
    if (getFileNameRegex().isPresent()) {
      writer.keyword("FOR");
      writer.keyword("REGEX");
      fileNameRegex.unparse(writer, leftPrec, rightPrec);
    }

    if (promotion.getValue() != null) {
      if (promotion.booleanValue()) {
        writer.keyword("AUTO");
      } else {
        writer.keyword("AVOID");
      }
      writer.keyword("PROMOTION");
    }

    if (forceUp.getValue() != null) {
      if (forceUp.booleanValue()) {
        writer.keyword("FORCE");
      } else {
        writer.keyword("LAZY");
      }
      writer.keyword("UPDATE");
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

    if (errorOnConcurrentRefresh.getValue() != null) {
      if (errorOnConcurrentRefresh.booleanValue()) {
        writer.keyword("ERROR");
        writer.keyword("ON");
        writer.keyword("CONCURRENT");
        writer.keyword("REFRESH");
      }
    }
  }

  private void unparseKeyValuePair(
      SqlNodeList pair, SqlWriter writer, int leftPrec, int rightPrec) {
    pair.get(0).unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    pair.get(1).unparse(writer, leftPrec, rightPrec);
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
        setPartitionKVMap();
        break;
      case 10:
        fileNameRegex = (SqlLiteral) operand;
        break;
      case 11:
        errorOnConcurrentRefresh = (SqlLiteral) operand;
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
        partitionList,
        fileNameRegex,
        errorOnConcurrentRefresh);
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

  public SqlLiteral getAllPartitionsRefresh() {
    return allPartitionsRefresh;
  }

  public SqlLiteral getFileRefresh() {
    return fileRefresh;
  }

  public SqlLiteral getPartitionRefresh() {
    return partitionRefresh;
  }

  public SqlNodeList getFilesList() {
    return filesList;
  }

  public SqlNodeList getPartitionList() {
    return partitionList;
  }

  public Optional<String> getFileNameRegex() {
    return Optional.ofNullable(fileNameRegex).map(p -> ((SqlLiteral) p).toValue());
  }

  public List<String> getFileNames() {
    List<String> fileNames =
        filesList.getList().stream()
            .map(o -> ((SqlLiteral) o).getValueAs(String.class))
            .collect(Collectors.toList());
    return fileNames;
  }

  public Map<String, String> getPartition() {
    return partitionKVMap;
  }

  private void setPartitionKVMap() {
    partitionKVMap = new LinkedHashMap<>();
    partitionList.forEach(
        node -> {
          final SqlNodeList pair = (SqlNodeList) node;
          final SqlIdentifier name = (SqlIdentifier) pair.get(0);
          final SqlLiteral value = (SqlLiteral) pair.get(1);

          if (value.getTypeName().equals(SqlTypeName.NULL)) {
            partitionKVMap.put(name.getSimple(), null);
          } else {
            partitionKVMap.put(name.getSimple(), value.getValueAs(String.class));
          }
        });
  }

  public boolean isPartialRefresh() {
    return !getFileNames().isEmpty() || !getPartition().isEmpty();
  }

  public boolean errorOnConcurrentRefresh() {
    return errorOnConcurrentRefresh.getValue() != null && errorOnConcurrentRefresh.booleanValue();
  }

  @Override
  public SqlToPlanHandler toPlanHandler() {
    return new RefreshDatasetHandler();
  }
}
