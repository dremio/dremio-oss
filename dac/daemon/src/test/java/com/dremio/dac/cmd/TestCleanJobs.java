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
package com.dremio.dac.cmd;

import static org.junit.Assert.assertEquals;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class TestCleanJobs extends CleanBaseTest {

  private HybridJobsService jobsService;

  @Before
  public void setUp() throws NamespaceException, IOException {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) l(JobsService.class);
  }

  @Test
  public void test() throws Exception {
    final SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    int totalJobs = 100;
    for (int i = 0; i < totalJobs; i++) {
      getJobDetails(ctas);
    }
    Thread.sleep(20);
    long beforeJob2TS = System.currentTimeMillis();
    getJobDetails(ctas);
    Thread.sleep(20);

    String result = "";
    getCurrentDremioDaemon().close();
    Optional<LocalKVStoreProvider> providerOptional =
        CmdUtils.getKVStoreProvider(getDACConfig().getConfig());
    try (LocalKVStoreProvider provider = providerOptional.get()) {
      provider.start();
      long diffBeforeJob2 = System.currentTimeMillis() - beforeJob2TS;
      result = Clean.deleteOldJobsAndProfiles(provider, diffBeforeJob2, TimeUnit.MILLISECONDS);
    }

    final String expectedReport =
        ""
            + "Completed. Deleted "
            + totalJobs
            + " jobs."
            + System.lineSeparator()
            + "\tJobAttempts: "
            + totalJobs
            + ", Attempts with failure: 0"
            + System.lineSeparator()
            + "\t"
            + Clean.OfflineProfileCleaner.class.getSimpleName()
            + " executions: "
            + totalJobs
            + ", "
            + "failures: 0"
            + System.lineSeparator()
            + "\t"
            + Clean.OfflineTmpDatasetVersionsCleaner.class.getSimpleName()
            + " executions: "
            + totalJobs
            + ", failures: 0"
            + System.lineSeparator();
    assertEquals(expectedReport, result);
  }

  private com.dremio.service.job.JobDetails getJobDetails(SqlQuery ctas)
      throws JobNotFoundException {
    final JobId jobId =
        submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(ctas).build());
    return jobsService.getJobDetails(
        JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
  }
}
