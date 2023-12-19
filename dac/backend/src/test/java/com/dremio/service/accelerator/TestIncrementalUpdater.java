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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.calcite.plan.RelOptUtil;
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
  public static class EventObserver implements StreamObserver<JobEvent> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private final DeferredException ex = new DeferredException();

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
    public void waitAndClose(long timeout, String errMsg) throws Exception {
      if(!latch.await(timeout, TimeUnit.SECONDS)){
        Assert.fail(errMsg);
      }
      ex.close();
    }

  }

  public static class LogicalRelPlanCaptureListener implements PlanTransformationListener {
    private final AtomicReference<RelNode> logicalRelPlan = new AtomicReference<>();

    @Override
    public void onPhaseCompletion(final PlannerPhase phase, final RelNode before, final RelNode after, final long millisTaken) {
      if (phase == PlannerPhase.LOGICAL) {
        logicalRelPlan.set(before);
      }
    }

    public RelNode getRelPlan() {
      return logicalRelPlan.get();
    }

  }
  @Test
  public void testSubstitutionShuttle() throws Exception {
    LocalJobsService localJobsService = l(LocalJobsService.class);
    DatasetPath datsetPath = new DatasetPath(ImmutableList.of("cp", "tpch/nation.parquet"));
    final EventObserver eventObserver = new EventObserver();
    final LogicalRelPlanCaptureListener logicalPlanTransformationListener = new LogicalRelPlanCaptureListener();

    localJobsService.submitJob(JobsServiceTestUtils.toSubmitJobRequest(JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery("select n_regionkey, max(n_nationkey) as max_nation from cp.\"tpch/nation.parquet\" group by n_regionkey", SYSTEM_USERNAME))
      .setQueryType(QueryType.JDBC)
      .setDatasetPath(datsetPath.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.newVersion())
      .build()), eventObserver, logicalPlanTransformationListener);

    eventObserver.waitAndClose(25, "Acceleration job was not completed within allowed timeout.");
    long currentTime = System.currentTimeMillis();
    RelNode newLogicalPlan = logicalPlanTransformationListener.getRelPlan().accept(new MaterializationShuttle(IncrementalUpdateUtils.UPDATE_COLUMN, true, new UpdateId().setLongUpdateId(currentTime).setType(MinorType.BIGINT)));
    assertNotNull(newLogicalPlan.getRowType().getField(IncrementalUpdateUtils.UPDATE_COLUMN, false, false));
  }

  @Test
  public void testAddDummyGroupingFieldShuttle() throws Exception {
    LocalJobsService localJobsService = l(LocalJobsService.class);
    DatasetPath datsetPath = new DatasetPath(ImmutableList.of("cp", "tpch/nation.parquet"));
    final EventObserver eventObserver = new EventObserver();
    final LogicalRelPlanCaptureListener logicalPlanTransformationListener = new LogicalRelPlanCaptureListener();
    String planBeforeTransform = "" +
      "LogicalAggregate(group=[{0}], max_nation=[MAX($1)])\n" +
      "  LogicalProject(n_regionkey=[$1], n_nationkey=[$0])\n" +
      "    ScanCrel(table=[cp.\"tpch/nation.parquet\"], snapshot=[], columns=[`n_nationkey`, `n_regionkey`], splits=[1])\n";
    String expectedPlanAfterTransform = "" +
      "LogicalProject(n_regionkey=[$0], max_nation=[$2])\n" +
      "  LogicalAggregate(group=[{0, 2}], max_nation=[MAX($1)])\n" +
      "    LogicalProject(n_regionkey=[$1], n_nationkey=[$0], $_dremio_$_dummy_$=[null:NULL])\n" +
      "      ScanCrel(table=[cp.\"tpch/nation.parquet\"], snapshot=[], columns=[`n_nationkey`, `n_regionkey`], splits=[1])\n";

    localJobsService.submitJob(JobsServiceTestUtils.toSubmitJobRequest(JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery("select n_regionkey, max(n_nationkey) as max_nation from cp.\"tpch/nation.parquet\" group by n_regionkey", SYSTEM_USERNAME))
      .setQueryType(QueryType.JDBC)
      .setDatasetPath(datsetPath.toNamespaceKey())
      .setDatasetVersion(DatasetVersion.newVersion())
      .build()), eventObserver, logicalPlanTransformationListener);

    eventObserver.waitAndClose(25, "Acceleration job was not completed within allowed timeout.");
    RelNode newLogicalPlan = logicalPlanTransformationListener.getRelPlan().accept(new IncrementalUpdateUtils.AddDummyGroupingFieldShuttle());
    assertEquals(planBeforeTransform, RelOptUtil.toString(logicalPlanTransformationListener.getRelPlan()).replaceAll("snapshot=\\[\\d+\\]","snapshot=[]"));
    assertEquals(expectedPlanAfterTransform, RelOptUtil.toString(newLogicalPlan).replaceAll("snapshot=\\[\\d+\\]","snapshot=[]"));
  }
}
