/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.service;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobDataFragmentWrapper;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.util.JSONUtil;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobDataFragment;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * Tests for job results store.
 */
public class TestJobResultsStore extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testJobResultStore() throws Exception {
    populateInitialData();
    final JobsService jobsService = l(JobsService.class);
    final DatasetPath ds1 = new DatasetPath("s.ds1");
    Job job1_0 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery("select * from LocalFS1.\"dac-sample1.json\" limit 10", ds1.toParentPathList(), DEFAULT_USERNAME))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("1"))
        .build(), NoOpJobStatusListener.INSTANCE);
    job1_0.getData().loadIfNecessary();
    JobDataFragment result = job1_0.getData().truncate(10);
    JobDataFragment storedResult = jobsService.getJob(job1_0.getJobId()).getData().truncate(10);
    for (Field column: result.getSchema()) {
      assertTrue(storedResult.getSchema().getFields().contains(column));
    }
    for (int i=0; i<result.getReturnedRowCount(); i++) {
      boolean found = false;
      List<Object> valuesFromResult = new ArrayList<>();
      for(Field c : result.getSchema()) {
        valuesFromResult.add(result.extractValue(c.getName(), i));
      }

      for (int j=0; j<storedResult.getReturnedRowCount(); j++) {
        List<Object> valuesFromStored = new ArrayList<>();
        for(Field c : storedResult.getSchema()) {
          valuesFromStored.add(storedResult.extractValue(c.getName(), j));
        }
        if (valuesFromResult.equals(valuesFromStored)) {
          found = true;
          break;
        }
      }
      assertTrue("Missing row numbered [" + i + "] from " + JSONUtil.toString(new JobDataFragmentWrapper(0, storedResult)), found);
    }
  }
}
