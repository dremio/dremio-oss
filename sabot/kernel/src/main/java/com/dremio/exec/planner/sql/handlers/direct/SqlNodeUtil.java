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
package com.dremio.exec.planner.sql.handlers.direct;

import com.dremio.exec.expr.fn.impl.RegexpUtil;
import com.dremio.exec.work.foreman.ForemanSetupException;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlWith;

public class SqlNodeUtil {

  private static final Pattern MATCH_ALL = Pattern.compile(".*", Pattern.DOTALL);

  @SuppressWarnings("unchecked")
  public static <T> T unwrap(Object o, Class<T> clazz) throws ForemanSetupException {
    if (clazz.isAssignableFrom(o.getClass())) {
      return (T) o;
    } else {
      throw new ForemanSetupException(
          String.format(
              "Failure trying to treat %s as type %s.",
              o.getClass().getSimpleName(), clazz.getSimpleName()));
    }
  }

  public static Pattern getPattern(SqlNode node) {
    if (node == null) {
      return MATCH_ALL;
    }

    if (!(node instanceof SqlCharStringLiteral)) {
      throw new IllegalArgumentException("You must provide a string literal.");
    }

    String str = ((SqlCharStringLiteral) node).toValue().trim();
    return Pattern.compile(
        RegexpUtil.sqlToRegexLike(str),
        Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.DOTALL);
  }

  public static String getQueryKind(SqlNode sqlNode) {
    // A few of these need special handling!
    if (sqlNode instanceof SqlOrderBy) {
      sqlNode = ((SqlOrderBy) sqlNode).query;
    } else if (sqlNode instanceof SqlWith) {
      sqlNode = ((SqlWith) sqlNode).body;
    }

    if (sqlNode == null) {
      return "unknown";
    }

    return sqlNode.getKind().lowerName;
  }

  // prevent instantiation
  private SqlNodeUtil() {}
}
