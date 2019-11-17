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

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;

public final class DremioSqlConformance extends SqlDelegatingConformance {

  public static final DremioSqlConformance INSTANCE = new DremioSqlConformance();

  private DremioSqlConformance() {
    super(SqlConformanceEnum.DEFAULT);
  }

  @Override
  public boolean isBangEqualAllowed() {
    return true;
  }

  @Override
  public boolean isGroupByAlias() {
    return true;
  }

  @Override
  public boolean isGroupByOrdinal() {
    return true;
  }

  @Override
  public boolean isHavingAlias() {
    return true;
  }

  @Override
  public boolean isSortByOrdinal() {
    return true;
  }

  @Override
  public boolean isSortByAlias() {
    return true;
  }

  @Override
  public boolean isPercentRemainderAllowed() {
    return true;
  }

  @Override
  public boolean allowNiladicParentheses() {
    return true;
  }

  @Override
  public boolean allowExplicitRowValueConstructor() {
    return true;
  }

}
