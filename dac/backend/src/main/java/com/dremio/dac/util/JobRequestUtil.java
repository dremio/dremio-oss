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

import com.dremio.dac.explore.model.VersionContextReq;
import com.dremio.service.job.SqlQuery;
import com.google.common.base.Strings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class to create SqlQuery (proto) */
public final class JobRequestUtil {

  private JobRequestUtil() {}

  public static SqlQuery createSqlQuery(String sql, String username) {
    return createSqlQuery(sql, Collections.<String>emptyList(), username);
  }

  /** Creates SqlQuery (Proto) - used to populate SubmitJobRequest */
  public static SqlQuery createSqlQuery(
      String sql,
      List<String> context,
      String username,
      String engineName,
      String sessionId,
      Map<String, VersionContextReq> sourceVersionMapping) {
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
    if (!Strings.isNullOrEmpty(sessionId)) {
      sqlQueryBuilder.setSessionId(sessionId);
    }
    if (sourceVersionMapping != null && !sourceVersionMapping.isEmpty()) {
      Map<String, SqlQuery.VersionContext> sourceVersionMappingAsProto =
          sourceVersionMapping.entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, e -> toVersionContextProto(e.getValue())));

      sqlQueryBuilder.putAllSourceVersionMapping(sourceVersionMappingAsProto);
    }
    return sqlQueryBuilder.build();
  }

  public static SqlQuery.VersionContext toVersionContextProto(VersionContextReq versionContextReq) {
    return SqlQuery.VersionContext.newBuilder()
        .setType(toVersionContextTypeProto(versionContextReq.getType()))
        .setValue(versionContextReq.getValue())
        .build();
  }

  public static SqlQuery.VersionContextType toVersionContextTypeProto(
      VersionContextReq.VersionContextType type) {
    switch (type) {
      case BRANCH:
        return SqlQuery.VersionContextType.BRANCH;
      case TAG:
        return SqlQuery.VersionContextType.TAG;
      case COMMIT:
        return SqlQuery.VersionContextType.BARE_COMMIT;
      default:
        throw new IllegalStateException("Unexpected value: " + type);
    }
  }

  public static SqlQuery createSqlQuery(String sql, List<String> context, String username) {
    return createSqlQuery(sql, context, username, null, null, null);
  }
}
