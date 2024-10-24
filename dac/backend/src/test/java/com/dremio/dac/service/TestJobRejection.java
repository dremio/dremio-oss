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
package com.dremio.dac.service;

import static com.dremio.exec.ExecConstants.MAX_FOREMEN_PER_COORDINATOR;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

import com.dremio.common.VM;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.spaces.SpacePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Submit enough jobs to get some of them rejected and make sure job submission doesn't block */
public class TestJobRejection extends BaseTestServer {

  @Test
  public void test() throws InterruptedException, NamespaceException {
    // only allow one query to run at a time
    setSystemOption(MAX_FOREMEN_PER_COORDINATOR, 1);

    // create a test space
    final String testSpace = "test_space";
    final SpaceConfig config = new SpaceConfig().setName(testSpace);
    l(NamespaceService.class)
        .addOrUpdateSpace(new SpacePath(config.getName()).toNamespaceKey(), config);

    // submit enough "CREATE VDS" queries to saturate the command pool and more
    final int numVds = VM.availableProcessors() * 2;
    Semaphore semaphore = new Semaphore(0);
    final AtomicInteger numFailed = new AtomicInteger();
    for (int i = 0; i < numVds; i++) {
      final String query =
          String.format("CREATE VDS %s.vds%d AS SELECT * FROM sys.version", testSpace, i);
      Thread thread =
          new Thread(
              () -> {
                try {
                  submitAndWaitUntilSubmitted(
                      JobRequest.newBuilder()
                          .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
                          .setQueryType(QueryType.UI_INTERNAL_RUN)
                          .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
                          .build());
                } catch (Exception e) {
                  numFailed.incrementAndGet();
                } finally {
                  semaphore.release();
                }
              });
      thread.start();
    }

    // all submitted jobs should complete
    assertTrue(
        "Not all submitted jobs completed after 1 minute",
        semaphore.tryAcquire(numVds, 1, MINUTES));
    assertTrue("none of the job submission failed", numFailed.get() > 0);
  }
}
