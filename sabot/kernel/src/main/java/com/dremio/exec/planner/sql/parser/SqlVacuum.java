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

import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

public abstract class SqlVacuum extends SqlCall implements SqlToPlanHandler.Creator {
  protected static final List<String> OPTION_KEYS = ImmutableList.of(
    "older_than",
    "retain_last"
  );

  protected final SqlNodeList optionsList;
  protected final SqlNodeList optionsValueList;
  protected String oldThanTimestamp;
  protected Integer retainLastValue;

  private SqlSelect sourceSelect;

  public SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  /**
   * Creates a SqlVacuum.
   */
  public SqlVacuum(
    SqlParserPos pos,
    SqlNodeList optionsList,
    SqlNodeList optionsValueList) {
    super(pos);
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;
    populateOptions(optionsList, optionsValueList);
  }

  public void setSourceSelect(SqlSelect select) {
    this.sourceSelect = select;
  }

  public abstract NamespaceKey getPath();

  public VacuumOptions getVacuumOptions() {
    long olderThanInMillis;
    if (oldThanTimestamp != null) {
      olderThanInMillis = SqlHandlerUtil.convertToTimeInMillis(oldThanTimestamp, pos);
    } else {
      long currentTime = System.currentTimeMillis();
      olderThanInMillis = currentTime - MAX_SNAPSHOT_AGE_MS_DEFAULT;
    }
    int retainLast = retainLastValue != null ? retainLastValue : MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
    return new VacuumOptions(VacuumOptions.Type.TABLE, olderThanInMillis, retainLast);
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

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validate(this.sourceSelect);
  }
}
