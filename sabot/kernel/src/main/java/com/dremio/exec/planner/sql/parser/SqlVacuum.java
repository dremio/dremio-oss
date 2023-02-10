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

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VacuumOption;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;


public class SqlVacuum extends SqlCall {
  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("VACUUM", SqlKind.OTHER) {
    @Override
    public SqlCall createCall(SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands) {
      Preconditions.checkArgument(operands.length == 3, "SqlVacuum.createCall() " +
        "has 3 operands!");
      return new SqlVacuum(
        pos,
        (SqlIdentifier) operands[0],
        (SqlNodeList) operands[1],
        (SqlNodeList) operands[2]);
    }
  };

  private static final long MAX_SNAPSHOT_AGE_MS_DEFAULT = 5 * 24 * 60 * 60 * 1000; // 5 days
  private static final int MIN_SNAPSHOTS_TO_KEEP_DEFAULT = 1;
  private static final List<String> OPTION_KEYS = ImmutableList.of(
    "older_than",
    "retain_last"
  );

  private final SqlIdentifier table;
  private final SqlNodeList optionsList;
  private final SqlNodeList optionsValueList;
  private String oldThanTimestamp;
  private Integer retainLastValue;

  /**
   * Creates a SqlVacuum.
   */
  public SqlVacuum(
    SqlParserPos pos,
    SqlIdentifier table,
    SqlNodeList optionsList,
    SqlNodeList optionsValueList) {
    super(pos);
    this.table = table;
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;
    populateOptions(optionsList, optionsValueList);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    final List<SqlNode> ops =
      ImmutableList.of(
        table,
        optionsList,
        optionsValueList);
    return ops;
  }

  public NamespaceKey getPath() {
    return new NamespaceKey(table.names);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("VACUUM");
    writer.keyword("TABLE");
    table.unparse(writer, leftPrec, rightPrec);

    writer.keyword("EXPIRE");
    writer.keyword("SNAPSHOTS");
    if(optionsList != null) {
      for (int i = 0; i < optionsList.size(); i++) {
        optionsList.get(i).unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        optionsValueList.get(i).unparse(writer, leftPrec, rightPrec);
      }
    }
  }

  public VacuumOption getVacuumOption() {
    Long olderThanInMillis = null;
    if (oldThanTimestamp != null) {
      olderThanInMillis = SqlHandlerUtil.convertToTimeInMillis(oldThanTimestamp, pos);
    } else {
      long currentTime = System.currentTimeMillis();
      olderThanInMillis = currentTime - MAX_SNAPSHOT_AGE_MS_DEFAULT;
    }
    int retainLast = retainLastValue != null ? retainLastValue : MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
    return new VacuumOption(VacuumOption.Type.TABLE, olderThanInMillis, retainLast);
  }

  private void populateOptions(SqlNodeList optionsList, SqlNodeList optionsValueList) {
    if (optionsList == null) {
      return;
    }

    int idx = 0;
    for (SqlNode option : optionsList) {
      SqlIdentifier optionIdentifier = (SqlIdentifier) option;
      String optionName = optionIdentifier.getSimple();
      switch (OPTION_KEYS.indexOf(optionName.toLowerCase())) {
        case 0:
          this.oldThanTimestamp = ((SqlLiteral) optionsValueList.get(idx)).getValueAs(String.class);
          break;
        case 1:
          SqlNumericLiteral optionValueNumLiteral = (SqlNumericLiteral) optionsValueList.get(idx);
          Integer retainLastValue = Integer.valueOf(optionValueNumLiteral.intValue(true));
          if (retainLastValue <= 0) {
            throw UserException.unsupportedError()
              .message("Minimum number of snapshots to retain can be 1")
              .buildSilently();
          }
          this.retainLastValue = retainLastValue;
          break;
        default:
          try {
            throw new SqlParseException(String.format("Unsupported option '%s' for VACUUM TABLE.", optionName), pos, null, null, null);
          } catch (SqlParseException e) {
            throw new RuntimeException(e);
          }
      }
      idx++;
    }
  }
}
