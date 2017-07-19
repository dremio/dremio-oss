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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.common.DeferredException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.FileMaterializationShuttle;
import com.dremio.service.accelerator.testing.JobStatusLogger;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.ImmutableList;

/**
 * TestIncrementalUpdater
 */
public class TestIncrementalUpdater extends BaseTestServer {
  @Test
  public void testSubstitutionShuttle() throws Exception {
    JobsService jobsService = l(JobsService.class);
    DatasetPath datsetPath = new DatasetPath(ImmutableList.of("cp", "tpch/nation.parquet"));
    final AtomicReference<RelNode> logicalPlan = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);
    final DeferredException ex = new DeferredException();
    Job job = jobsService.submitJob(
      new SqlQuery("select n_regionkey, max(n_nationkey) as max_nation from cp.\"tpch/nation.parquet\" group by n_regionkey", SYSTEM_USERNAME),
      QueryType.JDBC,
      datsetPath.toNamespaceKey(),
      new DatasetVersion(System.currentTimeMillis(), 0),
      new JobStatusLogger() {
        @Override
        public void planRelTansform(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
          if (phase == PlannerPhase.LOGICAL) {
            logicalPlan.set(before);
          }
        }

        @Override
        public void jobFailed(Exception paramException) {
          ex.addException(paramException);
          latch.countDown();
        }

        @Override
        public void jobCompleted() {
          latch.countDown();
        }

        @Override
        public void jobCancelled() {
          ex.addException(new RuntimeException("Job cancelled."));
          latch.countDown();
        }

      }
    );

    if(!latch.await(25, TimeUnit.SECONDS)){
      Assert.fail("Acceleration job was not completed within allowed timeout.");
    }
    ex.close();
    long currentTime = System.currentTimeMillis();
    RelNode newLogicalPlan = logicalPlan.get().accept(new FileMaterializationShuttle(currentTime));
    assertNotNull(newLogicalPlan.getRowType().getField(IncrementalUpdateUtils.UPDATE_COLUMN, false, false));
    job.getData().close();
  }
}
