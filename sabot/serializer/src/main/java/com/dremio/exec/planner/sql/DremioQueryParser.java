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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

/**
 * Simple Query Parser API.
 */
public abstract class DremioQueryParser {
  /**
   * Parse a sql string.
   * @param sql Sql to parse.
   * @return The validated SqlTree tree.
   */
  public abstract SqlNode parse(String sql);

  public RelNode toRel(String query) {
    final SqlNode sqlNode = parse(query);
    return convertSqlNodeToRel(sqlNode);
  }

  /**
   * Get the rel from a previously parsed sql tree.
   * @return The RelNode tree.
   */
  public abstract RelNode convertSqlNodeToRel(SqlNode sqlNode);
}
