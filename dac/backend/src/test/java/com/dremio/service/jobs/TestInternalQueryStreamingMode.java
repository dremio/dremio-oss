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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;
import com.dremio.service.job.SubmitJobRequest;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * Testing e2e internal query streamResultsMode
 */
public class TestInternalQueryStreamingMode extends BaseTestServer {
  private static final String SQL_QUERY = "SELECT \"employee_id\", \"full_name\" FROM cp.\"employees_with_null.json\"";

  private LocalJobsService jobsService;

  private static Server server;
  private static ManagedChannel channel;
  private static JobsServiceStub asyncStub;

  @Before
  public void setup() throws Exception {
    jobsService = l(LocalJobsService.class);

    final String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name)
      .directExecutor()
      .addService(new JobsServiceAdapter(p(LocalJobsService.class)))
      .addService(new Chronicle(p(LocalJobsService.class), () -> getSabotContext().getExecutorService()))
      .build();
    server.start();

    channel = InProcessChannelBuilder.forName(name)
      .directExecutor()
      .build();
    asyncStub = JobsServiceGrpc.newStub(channel);
  }

  @AfterClass
  public static void cleanUp() {
    if (server != null) {
      server.shutdown();
    }
    if (channel != null) {
      channel.shutdown();
    }
  }

  // JobEvent observer is instanceOf ServerCallStreamObserver, supports back pressure.
  @Test
  public void testStreamingEnabledWithScso() {
    final com.dremio.service.job.SqlQuery sqlQuery = com.dremio.service.job.SqlQuery.newBuilder()
      .setSql(SQL_QUERY)
      .setUsername(DEFAULT_USERNAME)
      .addAllContext(Collections.<String>emptyList())
      .build();
    final QueryResultObserver observer = new QueryResultObserver();
    asyncStub.submitJob(SubmitJobRequest.newBuilder().setSqlQuery(sqlQuery).setStreamResultsMode(true).build(), observer);
    observer.awaitUnchecked();
    assertTrue(observer.isCompleted());
    assertTrue(observer.getResults().size() > 0);
    assertEquals(5, observer.getTotalEvents());
  }

  // JobEvent observer is NOT instanceOf ServerCallStreamObserver, doesn't support back pressure.
  @Test
  public void testStreamingEnabledWithoutScso() {
    final JobRequest jobRequest = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(SQL_QUERY, DEFAULT_USERNAME))
      .setStreamResultsMode(true)
      .build();
    final QueryResultObserver observer = new QueryResultObserver();
    jobsService.submitJob(JobsServiceTestUtils.toSubmitJobRequest(jobRequest), observer, PlanTransformationListener.NO_OP);
    observer.awaitUnchecked();
    assertTrue(observer.isCompleted());
    assertTrue(observer.getResults().size() > 0);
    assertEquals(5, observer.getTotalEvents());
  }

  @Test
  public void testStreamingDisabled() {
    final JobRequest jobRequest = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(SQL_QUERY, DEFAULT_USERNAME))
      .build();
    final QueryResultObserver observer = new QueryResultObserver();
    jobsService.submitJob(JobsServiceTestUtils.toSubmitJobRequest(jobRequest), observer, PlanTransformationListener.NO_OP);
    observer.awaitUnchecked();
    assertTrue(observer.isCompleted());
    assertEquals(0, observer.getResults().size());
    assertEquals(4, observer.getTotalEvents());
  }

  /**
   * Observer for results.
   */
  public static class QueryResultObserver implements StreamObserver<JobEvent> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicInteger eventCount = new AtomicInteger(0);
    private List<ByteString> results = new ArrayList<>();

    private volatile Throwable ex;
    private volatile boolean completed = false;

    @Override
    public void onNext(JobEvent value) {
      eventCount.getAndIncrement();
      if (value.hasResultData()) {
        results.add(value.getResultData());
      } else if (value.hasFinalJobSummary()) {
        final JobSummary finalSummary = value.getFinalJobSummary();
        if (finalSummary.getJobState() == JobState.COMPLETED) {
          completed = true;
        }
        latch.countDown();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (t instanceof StatusRuntimeException) {
        final Optional<UserException> ue = GrpcExceptionUtil.fromStatusRuntimeException((StatusRuntimeException) t);
        if (ue.isPresent()) {
          ex = ue.get();
        } else {
          ex = t;
        }
      } else {
        ex = t;
      }
      latch.countDown();
    }

    @Override
    public void onCompleted() {
    }

    List<ByteString> getResults() {
      return results;
    }

    boolean isCompleted() {
      return completed;
    }

    int getTotalEvents() {
      return eventCount.get();
    }

    void awaitUnchecked() {
      try {
        latch.await();
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }

      if (ex != null) {
        Throwables.throwIfUnchecked(ex);
        throw new RuntimeException(ex);
      }
    }
  }
}
