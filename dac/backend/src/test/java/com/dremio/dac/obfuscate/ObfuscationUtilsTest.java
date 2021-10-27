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

package com.dremio.dac.obfuscate;

import static org.junit.Assert.assertEquals;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import com.dremio.context.RequestContext;
import com.dremio.context.SupportContext;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;

public class ObfuscationUtilsTest extends BaseTestServer {

  @Test
  public void obfuscateQueryProfilePositiveTest() throws Exception {
    ObfuscationUtils.setFullObfuscation(true);
    String query = "SELECT * FROM (VALUES(1234))";
    String observedQuery = obfuscateQueryProfileHelper(new SupportContext("dummy", "dummy"), query);
    String expectedQuery = "SELECT \"field_0\"\n" + "FROM \"Obfuscated\"";
    assertEquals(expectedQuery, observedQuery);
  }

  @Test
  public void obfuscateQueryProfileNegativeTest() throws Exception {
    String query = "SELECT * FROM (VALUES(1234))";
    String observedQuery = obfuscateQueryProfileHelper(null, query);
    String expectedQuery = query;
    assertEquals(expectedQuery, observedQuery);
  }

  /**
   * Run query, get query profile and return query string from query profile.
   * @param supportContext
   * @param query
   * @return
   * @throws Exception
   */
  private String obfuscateQueryProfileHelper(SupportContext supportContext, String query) throws Exception {
    return RequestContext.current()
      .with(SupportContext.CTX_KEY, supportContext)
      .call(() -> getQueryProfile(query).getQuery());
  }

  @NotNull
  private UserBitShared.QueryProfile getQueryProfile(String query) throws Exception {
    UserBitShared.QueryProfile queryProfile = getQueryProfile(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .setDatasetVersion(DatasetVersion.NONE)
        .build()
    );

    queryProfile = ObfuscationUtils.obfuscate(queryProfile);
    assertEquals(UserBitShared.QueryResult.QueryState.COMPLETED, queryProfile.getState());
    return queryProfile;
  }
}
