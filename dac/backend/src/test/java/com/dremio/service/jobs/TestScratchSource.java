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
package com.dremio.service.jobs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.job.proto.QueryType;
import com.dremio.test.TemporarySystemProperties;

/**
 * Test scratch source.
 */
public class TestScratchSource extends BaseTestServer {

  @Rule
  public final TemporarySystemProperties properties = new TemporarySystemProperties();

  private LocalJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (LocalJobsService) l(JobsService.class);
  }

  @Test
  public void testCTASAndDropTable() throws Exception {
    // Disabling auto promotion should still work with $scratch
    properties.set("dremio.datasets.auto_promote", "false");
    // Create a table
    final SqlQuery ctas = getQueryFromSQL("CREATE TABLE \"$scratch\".\"ctas\" AS select * from (VALUES (1))");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(ctas)
          .setQueryType(QueryType.UI_RUN)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    final FileSystemPlugin plugin = getCurrentDremioDaemon()
        .getBindingProvider()
        .lookup(CatalogService.class)
        .getSource("$scratch");

    // Make sure the table data files exist
    final File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctas");
    assertTrue(ctasTableDir.exists());
    assertTrue(ctasTableDir.list().length >= 1);

    // Now drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctas\""); // throws if not auto-promoted
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(dropTable)
          .setQueryType(QueryType.ACCELERATOR_DROP)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    // Make sure the table data directory is deleted
    assertFalse(ctasTableDir.exists());
  }
}
