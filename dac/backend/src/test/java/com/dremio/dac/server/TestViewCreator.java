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
package com.dremio.dac.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.ws.rs.client.Entity;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.model.folder.Folder;
import com.dremio.dac.model.spaces.Space;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;

/**
 *
 */
public class TestViewCreator extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void createQueryDrop() throws Exception {
    JobsService jobsService = l(JobsService.class);

    expectSuccess(getBuilder(getAPIv2().path("space/mySpace")).buildPut(Entity.json(new Space(null, "mySpace", null, null, null, 0, null))));

    expectSuccess(getBuilder(getAPIv2().path("space/mySpace/folder/")).buildPost(Entity.json("{\"name\": \"myFolder\"}")), Folder.class);

    Job job1 = jobsService.submitExternalJob(new SqlQuery("create view mySpace.myFolder.myView as select * from cp.nation_ctas.t1.\"0_0_0.parquet\"", DEFAULT_USERNAME), QueryType.UI_RUN);

    job1.getData().loadIfNecessary();

    Job job2 = jobsService.submitExternalJob(new SqlQuery("select * from mySpace.myFolder.myView", DEFAULT_USERNAME), QueryType.UI_RUN);
    job2.getData().loadIfNecessary();
    assertEquals(25, job2.getJobAttempt().getDetails().getOutputRecords().longValue());

    Job job3 = jobsService.submitExternalJob(new SqlQuery("drop view mySpace.myFolder.myView", DEFAULT_USERNAME), QueryType.UI_RUN);
    job3.getData().loadIfNecessary();

    Job job4 = jobsService.submitExternalJob(new SqlQuery("select * from mySpace.myFolder.myView", DEFAULT_USERNAME), QueryType.UI_RUN);

    try {
      job4.getData().loadIfNecessary();
      Assert.fail("query should have failed");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("not found"));
    }
  }
}
