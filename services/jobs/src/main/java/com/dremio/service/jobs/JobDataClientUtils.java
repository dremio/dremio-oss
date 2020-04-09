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

import static com.dremio.service.jobs.RecordBatchHolder.newRecordBatchHolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorContainer;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.stub.StreamObserver;

/**
 * Utility class for getting job data
 */
public final class JobDataClientUtils {

  private JobDataClientUtils() {
  }

  /**
   * Streams JobData from server over gRPC and creates list of RecordBatchHolder to populate JobDataFragment
   * @param stream flight stream for a particular job
   * @param allocator allocator for vectors
   * @param offset start index of results
   * @param limit max number of results to fetch
   * @return
   */
  public static List<RecordBatchHolder> getData(FlightStream stream, BufferAllocator allocator, int offset, int limit) {
    final List<RecordBatchHolder> batches = new ArrayList<>();
    try (final VectorContainer container = new VectorContainer(allocator);
         final VectorSchemaRoot root = stream.getRoot()) {

      int runningCount = 0;
      int remaining = limit;

      while (stream.next()) {
        VectorContainer.transferFromRoot(root, container, allocator);

        final int currentBatchCount = root.getRowCount();
        runningCount += currentBatchCount;

        // skip batch if offset is beyond
        if (offset >= runningCount) {
          continue;
        }
        // start at start of batch or the offset minus the start index of the current batch
        final int batchStart = Math.max(0, offset - (runningCount - currentBatchCount));
        // end at the end of batch or the at the limit
        final int batchEnd = Math.min(currentBatchCount, batchStart + remaining);

        final RecordBatchHolder batchHolder = newRecordBatchHolder(
          new RecordBatchData(container, allocator),
          batchStart,
          batchEnd
        );
        batches.add(batchHolder);
        remaining -= batchHolder.size();

        // break if we hit the limit
        if (remaining == 0) {
          break;
        }
      }

      // If no batches/records, return an empty result
      if (batches.isEmpty()) {
        container.setRecordCount(0);
        container.buildSchema();
        batches.add(newRecordBatchHolder(new RecordBatchData(container, allocator), 0, 0));
      }
    }
    return batches;
  }

  /**
   * Factory for returning JobData over gRPC
   * @param jobsService reference to job service
   * @param bufferAllocator allocator for vectors
   * @param jobId jobid for corresponding jobresults
   * @param offset start index of results
   * @param limit max number of results to fetch
   * @return JobDataFragment
   */
  public static JobDataFragment getJobData(JobsService jobsService, BufferAllocator bufferAllocator, JobId jobId, int offset, int limit) {
    final FlightClient flightClient = jobsService.getJobsClient().getFlightClient();
    final Ticket ticket = new JobsFlightTicket(jobId.getId(), offset, limit).toTicket();
    try (FlightStream flightStream = flightClient.getStream(ticket)) {
      return new JobDataFragmentImpl(new RecordBatches(JobDataClientUtils.getData(
        flightStream, bufferAllocator, offset, limit)), offset, jobId);
    } catch (FlightRuntimeException fre) {
      Optional<UserException> ue = JobsRpcUtils.fromFlightRuntimeException(fre);
      throw ue.isPresent() ? ue.get() : fre;
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait for BatchSchema (Query Metadata) to be retrievable via JobDetails. Use this
   * method sparingly, prefer reusing the listener from Job submission instead.
   */
  public static void waitForBatchSchema(JobsService jobsService, final JobId jobId) {
    final SettableFuture<Void> settableFuture = SettableFuture.create();

    jobsService.getJobsClient()
      .getAsyncStub()
      .subscribeToJobEvents(JobsProtoUtil.toBuf(jobId), new StreamObserver<JobEvent>() {
        @Override
        public void onNext(JobEvent value) {
          if (value.hasQueryMetadata() || value.hasFinalJobSummary()) {
            settableFuture.set(null);
          }
        }

        @Override
        public void onError(Throwable t) {
          settableFuture.setException(t);
        }

        @Override
        public void onCompleted() {

        }
      });

    try {
      settableFuture.get();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait for the final Job state. This corresponds to Job#isCompleted, which means the
   * Job may be successful, cancelled, or failed. Use this method sparingly, prefer
   * reusing the listener from Job submission instead.
   */
  public static void waitForFinalState(JobsService jobsService, final JobId jobId) {
    final SettableFuture<Void> settableFuture = SettableFuture.create();

    jobsService.getJobsClient()
        .getAsyncStub()
        .subscribeToJobEvents(JobsProtoUtil.toBuf(jobId), new StreamObserver<JobEvent>() {
          @Override
          public void onNext(JobEvent value) {
            if (value.getEventCase() == JobEvent.EventCase.FINAL_JOB_SUMMARY) {
              settableFuture.set(null);
            }
          }

          @Override
          public void onError(Throwable t) {
            settableFuture.setException(t);
          }

          @Override
          public void onCompleted() {
          }
        });

    try {
      settableFuture.get();
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
