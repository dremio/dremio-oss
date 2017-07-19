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
package com.dremio.service.accelerator;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.ImmutableList;

/**
 * Ensures following system tables work properly: sys.accelerations, sys.layouts, and sys.materializations
 */
public class TestAccelerationListManager extends BaseTestServer {
  private static final NamespaceKey NONE_PATH = new NamespaceKey(ImmutableList.of("__none"));

  private LocalJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (LocalJobsService) l(JobsService.class);
  }

  @Test
  public void testAccelerationsTable() {
    jobsService.submitJob(
      getQueryFromSQL("SELECT * FROM sys.accelerations"),
      QueryType.UNKNOWN,
      NONE_PATH,
      DatasetVersion.NONE,
      JobStatusListener.NONE
    ).getData().loadIfNecessary();
  }

  @Test
  public void testLayoutsTable() {
    jobsService.submitJob(
      getQueryFromSQL("SELECT * FROM sys.layouts"),
      QueryType.UNKNOWN,
      NONE_PATH,
      DatasetVersion.NONE,
      JobStatusListener.NONE
    ).getData().loadIfNecessary();
  }

  @Test
  public void testMaterializationsTable() {
    jobsService.submitJob(
      getQueryFromSQL("SELECT * FROM sys.materializations"),
      QueryType.UNKNOWN,
      NONE_PATH,
      DatasetVersion.NONE,
      JobStatusListener.NONE
    ).getData().loadIfNecessary();
  }
}
