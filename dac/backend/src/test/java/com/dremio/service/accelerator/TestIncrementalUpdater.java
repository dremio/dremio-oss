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
import com.dremio.common.types.MinorType;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils.MaterializationShuttle;
import com.dremio.proto.model.UpdateId;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.PlanTransformationListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

/**
 * TestIncrementalUpdater
 */
public class TestIncrementalUpdater extends BaseTestServer {
  @Test
  public void testSubstitutionShuttle() throws Exception {
    LocalJobsService localJobsService = l(LocalJobsService.class);
    DatasetPath datsetPath = new DatasetPath(ImmutableList.of("cp", "tpch/nation.parquet"));
    final AtomicReference<RelNode> logicalPlan = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);
    final DeferredException ex = new DeferredException();
    final StreamObserver<JobEvent> eventObserver = new StreamObserver<JobEvent>() {
      @Override
      public void onNext(JobEvent value) {
        if (value.hasFinalJobSummary()) {
          if (value.getFinalJobSummary().getJobState() == JobState.CANCELED) {
            ex.addException(new RuntimeException("Job cancelled."));
          }
          latch.countDown();
        }
      }

      @Override
      public void onError(Throwable t) {
        if (t instanceof RuntimeException) {
          ex.addException((RuntimeException) t);
        } else {
          ex.addException(new RuntimeException(t));
        }
        latch.countDown();
      }

      @Override
      public void onCompleted() {
      }
    };
    final PlanTransformationListener planTransformationListener = new PlanTransformationListener() {
      @Override
      public void onPhaseCompletion(PlannerPhase phase, RelNode before, RelNode after, long millisTaken) {
        if (phase == PlannerPhase.LOGICAL) {
          logicalPlan.set(before);
        }
      }
    };

    localJobsService.submitJob(JobsServiceTestUtils.toSubmitJobRequest(JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery("select n_regionkey, max(n_nationkey) as max_nation from cp.\"tpch/nation.parquet\" group by n_regionkey", SYSTEM_USERNAME))
      .setQueryType(QueryType.JDBC)
      .setDatasetPath(datsetPath.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.newVersion())
      .build()), eventObserver, planTransformationListener);

    if(!latch.await(25, TimeUnit.SECONDS)){
      Assert.fail("Acceleration job was not completed within allowed timeout.");
    }
    ex.close();
    long currentTime = System.currentTimeMillis();
    RelNode newLogicalPlan = logicalPlan.get().accept(new MaterializationShuttle(IncrementalUpdateUtils.UPDATE_COLUMN, true, new UpdateId().setLongUpdateId(currentTime).setType(MinorType.BIGINT)));
    assertNotNull(newLogicalPlan.getRowType().getField(IncrementalUpdateUtils.UPDATE_COLUMN, false, false));
  }
}
