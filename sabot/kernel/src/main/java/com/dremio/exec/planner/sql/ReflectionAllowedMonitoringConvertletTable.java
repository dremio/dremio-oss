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
package com.dremio.exec.planner.sql;

import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

import com.google.common.collect.ImmutableSet;

/**
 * Determine whether we allow this rel in a reflection.
 */
public class ReflectionAllowedMonitoringConvertletTable implements SqlRexConvertletTable {

  private Set<SqlOperator> WHITELIST = ImmutableSet.of(
      SqlStdOperatorTable.CURRENT_TIME,
      SqlStdOperatorTable.CURRENT_DATE,
      SqlStdOperatorTable.CURRENT_TIMESTAMP,
      SqlStdOperatorTable.LOCALTIME,
      SqlStdOperatorTable.LOCALTIMESTAMP,
      SqlStdOperatorTable.PI, // here until CALCITE-2750 is fixed and Dremio has it.
      DremioSqlOperatorTable.NOW
      );

  private final ConvertletTableNotes convertletTableNotes;

  public ReflectionAllowedMonitoringConvertletTable(ConvertletTableNotes convertletTableNotes) {
    super();
    this.convertletTableNotes = convertletTableNotes;
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlOperator operator = call.getOperator();
    if(operator.isDynamicFunction() || !operator.isDeterministic()) {
      convertletTableNotes.planCacheable = false;
    }
    if (operator.isDynamicFunction() && !WHITELIST.contains(operator)) {
      convertletTableNotes.contextSensitive = true;
    }
    return null;
  }

  public static final class ConvertletTableNotes {
    private boolean contextSensitive = false;
    private boolean planCacheable = true;

    public boolean isReflectionDisallowed() {
      return contextSensitive;
    }

    public boolean isPlanCacheable() {
      return planCacheable;
    }
  }
}
