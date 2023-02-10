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
package com.dremio.dac.server;

import static com.dremio.service.users.SystemUser.SYSTEM_USER;

import java.util.UUID;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;

import org.junit.BeforeClass;

import com.dremio.common.util.TestTools;
import com.dremio.dac.api.MetadataPolicy;
import com.dremio.dac.api.Source;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;

/**
 * Base test class
 */
public class TestSystemAutoPromotionBase extends BaseTestServer {

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    BaseTestServer.getPopulator().populateTestUsers();
  }

  protected void run() {
    // Create a source with auto promotion disabled
    NASConf nasConf = new NASConf();
    nasConf.path = TestTools.getWorkingPath() + "/src/test/resources";

    MetadataPolicy policy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    policy.setAutoPromoteDatasets(false);
    Source source = new Source();
    source.setName("no_promote");
    source.setType("NAS");
    source.setConfig(nasConf);
    source.setMetadataPolicy(policy);

    // Create the source
    expectSuccess(getBuilder(getPublicAPI(3).path("/catalog/")).buildPost(Entity.json(source)), new GenericType<Source>() {});

    String random = UUID.randomUUID().toString();
    try {
      // Create a table
      submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(String.format("create table \"%s\".\"newTable-%s\" as select 1",
            source.getName(), random),
            SYSTEM_USER.getUserName()))
          .setQueryType(QueryType.UI_RUN)
          .build()
      );
    } finally {
      submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(String.format("drop table \"%s\".\"newTable-%s\"",
            source.getName(), random),
            SYSTEM_USER.getUserName()))
          .setQueryType(QueryType.UI_RUN)
          .build()
      );
    }
  }
}
