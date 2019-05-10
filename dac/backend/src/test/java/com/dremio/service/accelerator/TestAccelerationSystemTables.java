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
package com.dremio.service.accelerator;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * Ensures following system tables work properly: sys.reflections, sys.dependencies, sys.refreshes, and sys.materializations
 */
public class TestAccelerationSystemTables extends BaseTestServer {
  private static final NamespaceKey NONE_PATH = new NamespaceKey(ImmutableList.of("__none"));

  private LocalJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (LocalJobsService) l(JobsService.class);
  }

  @Test
  public void testReflectionsTable() {
    JobsServiceUtil.waitForJobCompletion(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("SELECT * FROM sys.reflections"))
        .setDatasetPath(NONE_PATH)
        .build(), NoOpJobStatusListener.INSTANCE));
  }

  @Test
  public void testMaterializationsTable() {
    JobsServiceUtil.waitForJobCompletion(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("SELECT * FROM sys.materializations"))
        .setDatasetPath(NONE_PATH)
        .build(), NoOpJobStatusListener.INSTANCE));
  }

  @Test
  public void testDependenciesTable() {
    JobsServiceUtil.waitForJobCompletion(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("SELECT * FROM sys.dependencies"))
        .setDatasetPath(NONE_PATH)
        .build(), NoOpJobStatusListener.INSTANCE));
  }

  @Test
  public void testRefreshesTable() {
    JobsServiceUtil.waitForJobCompletion(jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("SELECT * FROM sys.refreshes"))
        .setDatasetPath(NONE_PATH)
        .build(), NoOpJobStatusListener.INSTANCE));
  }
}
