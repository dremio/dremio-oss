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
package com.dremio.exec.planner.sql.evaluator;

import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.CURRENT_DATE_UTC;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.CURRENT_TIME_UTC;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.LAST_QUERY_ID;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.NOW;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.QUERY_USER;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.SESSION_USER;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.SYSTEM_USER;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.UNIX_TIMESTAMP;
import static com.dremio.exec.planner.sql.DremioSqlOperatorTable.USER;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_DATE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_SCHEMA;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.CURRENT_TIMESTAMP;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIME;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LOCALTIMESTAMP;

import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlOperator;

/**
 * Maps from SqlOperator to FunctionEval. These are needed for the FunctionEvaluatorUtil to know
 * which FunctionEval to execute for each SqlOperator it encounters.
 */
public final class FunctionEvalMaps {
  public static final ImmutableMap<SqlOperator, FunctionEval> SYSTEM =
      ImmutableMap.of(
          DremioSqlOperatorTable.IDENTITY, IdentityEvaluator.INSTANCE,
          DremioSqlOperatorTable.TYPEOF, TypeOfEvaluator.INSTANCE);

  public static final ImmutableMap<SqlOperator, FunctionEval> DYNAMIC =
      new ImmutableMap.Builder<SqlOperator, FunctionEval>()
          .put(CURRENT_DATE, CurrentDateEvaluator.INSTANCE)
          .put(CURRENT_DATE_UTC, CurrentDateUtcEvaluator.INSTANCE)
          .put(CURRENT_SCHEMA, CurrentSchemaEvaluator.INSTANCE)
          .put(CURRENT_TIME, CurrentTimeEvaluator.INSTANCE)
          .put(CURRENT_TIME_UTC, CurrentTimeUtcEvaluator.INSTANCE)
          .put(CURRENT_TIMESTAMP, CurrentTimestampEvaluator.INSTANCE)
          .put(LAST_QUERY_ID, LastQueryIdEvaluator.INSTANCE)
          .put(LOCALTIME, CurrentTimeEvaluator.INSTANCE)
          .put(LOCALTIMESTAMP, CurrentTimestampEvaluator.INSTANCE)
          .put(NOW, CurrentTimestampEvaluator.INSTANCE)
          .put(QUERY_USER, UserEvaluator.INSTANCE)
          .put(SESSION_USER, UserEvaluator.INSTANCE)
          .put(SYSTEM_USER, UserEvaluator.INSTANCE)
          .put(UNIX_TIMESTAMP, UnixTimestampEvaluator.INSTANCE)
          .put(USER, UserEvaluator.INSTANCE)
          .build();

  public static final ImmutableMap<SqlOperator, FunctionEval> ALL =
      ImmutableMap.<SqlOperator, FunctionEval>builder().putAll(SYSTEM).putAll(DYNAMIC).build();

  private FunctionEvalMaps() {}
}
