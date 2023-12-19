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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.VacuumOptions;
import com.dremio.exec.planner.sql.handlers.SqlHandlerUtil;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableMap;

public abstract class SqlVacuum extends SqlCall implements SqlToPlanHandler.Creator {
  public static final long MAX_FILE_AGE_MS_DEFAULT = TimeUnit.DAYS.toMillis(3);  // 3 days

  protected final SqlNodeList optionsList;
  protected final SqlNodeList optionsValueList;
  protected String olderThanTimestamp;
  protected Integer retainLastSnapshots;
  protected String location;

  private final Map<String, Consumer<SqlNode>> optionsConsumer;
  protected SqlLiteral expireSnapshots;
  protected SqlLiteral removeOrphans;

  /**
   * Creates a SqlVacuum.
   */
  public SqlVacuum(
    SqlParserPos pos,
    SqlLiteral expireSnapshots,
    SqlLiteral removeOrphans,
    SqlNodeList optionsList,
    SqlNodeList optionsValueList) {
    super(pos);
    this.expireSnapshots = expireSnapshots;
    this.removeOrphans = removeOrphans;
    this.optionsList = optionsList;
    this.optionsValueList = optionsValueList;
    this.optionsConsumer = ImmutableMap.of(
      "older_than", sqlNode -> this.olderThanTimestamp = extractStringValue(sqlNode),
      "retain_last", sqlNode -> this.retainLastSnapshots = extractRetainLast(sqlNode),
      "location", sqlNode -> this.location = extractStringValue(sqlNode));

    populateOptions(optionsList, optionsValueList);
  }

  public abstract NamespaceKey getPath();

  public Optional<Consumer<SqlNode>> getOptionsConsumer(String optionName) {
    return Optional.ofNullable(optionsConsumer.get(optionName));
  }

  public VacuumOptions getVacuumOptions() {
    long olderThanInMillis;
    if (expireSnapshots.booleanValue()) {
      if (olderThanTimestamp != null) {
        olderThanInMillis = SqlHandlerUtil.convertToTimeInMillis(olderThanTimestamp, pos);
      } else {
        long currentTime = System.currentTimeMillis();
        olderThanInMillis = currentTime - MAX_SNAPSHOT_AGE_MS_DEFAULT;
      }
      int retainLast = retainLastSnapshots != null ? retainLastSnapshots : MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
      return new VacuumOptions(expireSnapshots.booleanValue(), removeOrphans.booleanValue(), olderThanInMillis, retainLast, null, null);
    }

    if (removeOrphans.booleanValue()) {
      if (olderThanTimestamp != null) {
        olderThanInMillis = SqlHandlerUtil.convertToTimeInMillis(olderThanTimestamp, pos);
      } else {
        long currentTime = System.currentTimeMillis();
        // By default, try to clean orphan files which are at least 3 days old.
        olderThanInMillis = currentTime - MAX_FILE_AGE_MS_DEFAULT;
      }

      return new VacuumOptions(expireSnapshots.booleanValue(), removeOrphans.booleanValue(), olderThanInMillis, 1, location, null);
    }

    return new VacuumOptions(expireSnapshots.booleanValue(), removeOrphans.booleanValue(), null, null, null, null);
  }

  protected void populateOptions(SqlNodeList optionsList, SqlNodeList optionsValueList) {
    if (optionsList == null) {
      return;
    }

    int idx = 0;
    for (SqlNode option : optionsList) {
      SqlIdentifier optionIdentifier = (SqlIdentifier) option;
      String optionName = optionIdentifier.getSimple();

      Consumer<SqlNode> optionConsumer = getOptionsConsumer(optionName.toLowerCase())
        .orElseThrow(() -> new RuntimeException(new SqlParseException(
          String.format("Unsupported option '%s' for VACUUM.", optionName), pos, null, null, null)));

      optionConsumer.accept(optionsValueList.get(idx));
      idx++;
    }
  }

  protected String extractStringValue(SqlNode sqlNode) {
    return ((SqlLiteral) sqlNode).getValueAs(String.class);
  }

  protected Integer extractRetainLast(SqlNode sqlNode) {
    SqlNumericLiteral optionValueNumLiteral = (SqlNumericLiteral) sqlNode;
    Integer retainLastValue = Integer.valueOf(optionValueNumLiteral.intValue(true));
    if (retainLastValue <= 0) {
      throw UserException.unsupportedError()
        .message("Minimum number of snapshots to retain can be 1")
        .buildSilently();
    }
    return retainLastValue;
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    // No custom select clause. Hence, skip validation.
  }
}
