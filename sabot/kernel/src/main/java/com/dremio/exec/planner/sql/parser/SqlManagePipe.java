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

import static com.dremio.exec.planner.sql.parser.SqlCopyIntoTable.isOnErrorHandlingRequested;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.handlers.query.CopyIntoTableContext;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class SqlManagePipe extends SqlCall {

  protected final SqlIdentifier pipeName;
  protected final SqlCopyIntoTable sqlCopyInto;
  protected final SqlNumericLiteral dedupLookbackPeriod;
  protected final SqlIdentifier notificationProvider;
  protected final SqlIdentifier notificationQueueRef;

  public SqlManagePipe(
      SqlParserPos pos,
      SqlIdentifier pipeName,
      SqlNode sqlCopyInto,
      SqlNumericLiteral dedupLookbackPeriod,
      SqlIdentifier notificationProvider,
      SqlIdentifier notificationQueueRef) {
    super(pos);
    this.pipeName = pipeName;
    this.sqlCopyInto = (SqlCopyIntoTable) sqlCopyInto;
    this.dedupLookbackPeriod = dedupLookbackPeriod;
    this.notificationProvider = notificationProvider;
    this.notificationQueueRef = notificationQueueRef;
    validateCopyInto(sqlCopyInto);
  }

  public SqlIdentifier getPipeName() {
    return pipeName;
  }

  public SqlCopyIntoTable getSqlCopyInto() {
    return sqlCopyInto;
  }

  public Optional<Integer> getDedupLookbackPeriod() {
    return Optional.ofNullable(dedupLookbackPeriod)
        .map(dedupLookbackPeriod -> dedupLookbackPeriod.intValue(true));
  }

  public Optional<SqlIdentifier> getNotificationProvider() {
    return Optional.ofNullable(notificationProvider);
  }

  public Optional<SqlIdentifier> getNotificationQueueRef() {
    return Optional.ofNullable(notificationQueueRef);
  }

  private void validateCopyInto(SqlNode sqlCopyInto) {
    SqlCopyIntoTable copyIntoNode = (SqlCopyIntoTable) sqlCopyInto;
    if (copyIntoNode.getFiles().size() != 0 || copyIntoNode.getFilePattern().isPresent()) {
      throw UserException.parseError()
          .message("FILES and REGEX options are not allowed for PIPE commands.")
          .buildSilently();
    }

    List<String> copyOptionsList = copyIntoNode.getOptionsList();
    if (copyOptionsList.stream()
            .anyMatch(str -> str.equalsIgnoreCase(CopyIntoTableContext.CopyOption.ON_ERROR.name()))
        && !isOnErrorHandlingRequested(copyOptionsList, copyIntoNode.getOptionsValueList())) {
      throw UserException.parseError()
          .message("ON_ERROR must be set to CONTINUE or SKIP_FILE for PIPE commands.")
          .buildSilently();
    }
  }
}
