/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

public class DremioSqlConformance implements SqlConformance {

  public static DremioSqlConformance INSTANCE = new DremioSqlConformance();

  @Override
  public boolean isSortByOrdinal() {
    return SqlConformanceEnum.DEFAULT.isSortByOrdinal();
  }

  @Override
  public boolean isSortByAlias() {
    return SqlConformanceEnum.DEFAULT.isSortByAlias();
  }

  @Override
  public boolean isSortByAliasObscures() {
    return SqlConformanceEnum.DEFAULT.isSortByAliasObscures();
  }

  @Override
  public boolean isFromRequired() {
    return SqlConformanceEnum.DEFAULT.isFromRequired();
  }

  @Override
  public boolean isBangEqualAllowed() {
    return true;
  }

  @Override
  public boolean isMinusAllowed() {
    return SqlConformanceEnum.DEFAULT.isMinusAllowed();
  }

  @Override
  public boolean isApplyAllowed() {
    return SqlConformanceEnum.DEFAULT.isApplyAllowed();
  }
}
