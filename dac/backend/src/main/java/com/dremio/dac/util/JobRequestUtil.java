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
package com.dremio.dac.util;

import java.util.Collections;
import java.util.List;

import com.dremio.service.job.SqlQuery;
import com.google.common.base.Strings;

/**
 * Utility class to create SqlQuery (proto)
 */
public final class JobRequestUtil {

  private JobRequestUtil() {}

  public static SqlQuery createSqlQuery(String sql, String username) {
    return createSqlQuery(sql, Collections.<String>emptyList(), username);
  }

  /**
   * Creates SqlQuery (Proto) - used to populate SubmitJobRequest
   */
  public static SqlQuery createSqlQuery(String sql, List<String> context, String username, String engineName) {
    final SqlQuery.Builder sqlQueryBuilder = SqlQuery.newBuilder();
    if (!Strings.isNullOrEmpty(sql)) {
      sqlQueryBuilder.setSql(sql);
    }
    if (context != null) {
      sqlQueryBuilder.addAllContext(context);
    }
    if (!Strings.isNullOrEmpty(username)) {
      sqlQueryBuilder.setUsername(username);
    }
    if (!Strings.isNullOrEmpty(engineName)) {
      sqlQueryBuilder.setEngineName(engineName);
    }
    return sqlQueryBuilder.build();
  }

  public static SqlQuery createSqlQuery(String sql, List<String> context, String username) {
    return createSqlQuery(sql, context, username, null);
  }
}
