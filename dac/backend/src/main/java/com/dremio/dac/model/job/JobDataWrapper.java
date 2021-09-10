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
package com.dremio.dac.model.job;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.FlightProtos.CoordinatorFlightTicket;
import com.dremio.exec.proto.FlightProtos.JobsFlightTicket;
import com.dremio.exec.record.RecordBatchHolder;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobDataClientUtils;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsRpcUtils;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.RecordBatches;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A UI wrapper around JobData to allow for serialization
 */
public class JobDataWrapper implements JobData {
  private static final Logger logger = LoggerFactory.getLogger(JobDataWrapper.class);

  private final JobsService jobsService;
  private final JobId jobId;
  private final String userName;

  public JobDataWrapper(JobsService jobsService, JobId jobId, String userName) {
      this.jobsService = jobsService;
      this.jobId = jobId;
      this.userName = userName;
  }

  @Override
  public void close() {
  }

  /**
   * Get range of job data. Consumers must wait for data before calling this.
   */
  @Override
  public JobDataFragment range(BufferAllocator allocator, int offset, int limit) {
    return getJobData(Preconditions.checkNotNull(jobsService), allocator,
      Preconditions.checkNotNull(jobId), offset, limit);
  }

  /**
   * Get truncated range of job data. Consumers must wait for data before calling this.
   */
  @Override
  public JobDataFragment truncate (BufferAllocator allocator, int maxRows) {
    return getJobData(Preconditions.checkNotNull(jobsService), allocator,
      Preconditions.checkNotNull(jobId), 0, maxRows);
  }

  @Override
  public JobId getJobId() {
    return jobId;
  }

  @Override
  public String getJobResultsTable() {
    final JobDetailsRequest jobDetailsRequest = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setUserName(userName)
      .setProvideResultInfo(true)
      .build();
    try {
      return jobsService.getJobDetails(jobDetailsRequest).getJobResultTableName();
    } catch (JobNotFoundException e) {
      throw new IllegalArgumentException("Could not find Job.");
    }
  }

  public static JobDataFragmentWrapper getJobData(JobsService jobsService, BufferAllocator allocator, JobId jobId, int offset, int limit) {
    final Ticket ticket = new Ticket(CoordinatorFlightTicket.newBuilder()
      .setJobsFlightTicket(JobsFlightTicket.newBuilder().setJobId(jobId.getId()).setOffset(offset).setLimit(limit).build())
      .build().toByteArray());
    try (final FlightStream stream = jobsService.getJobsClient().getFlightClient()
      .getStream(ticket)) {
      List<RecordBatchHolder> batches = JobDataClientUtils.getData(stream, allocator, limit);
      return new JobDataFragmentWrapper(offset, ReleasingData.from(new RecordBatches(batches), jobId));
    } catch (FlightRuntimeException fre) {
      Optional<UserException> ue = JobsRpcUtils.fromFlightRuntimeException(fre);
      throw ue.isPresent() ? ue.get() : fre;
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
