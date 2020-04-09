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

import static org.apache.arrow.util.Preconditions.checkNotNull;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.service.job.proto.JobProtobuf;

import io.grpc.Status;

/**
 * Arrow Flight Producer for JobsService
 */
public class JobsFlightProducer implements FlightProducer, AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(JobsFlightProducer.class);
  private final LocalJobsService jobsService;
  private final BufferAllocator allocator;

  JobsFlightProducer(LocalJobsService jobsService, BufferAllocator allocator) {
    this.jobsService = jobsService;
    this.allocator = checkNotNull(allocator).newChildAllocator("jobs-flight-producer", 0, Long.MAX_VALUE);
  }

  @Override
  public void close() throws Exception {
    allocator.close();
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    /* Note that we do not trim record batches that we receive from the Job Results Store. This may result
     * in sending record that the client does not care about, or in the case of sequential requests, sending
     * duplicate records. We may want to trim the record batches if this presents a problem.
     */
    try {
      final JobsFlightTicket jobsFlightTicket = JobsFlightTicket.from(ticket);
      final JobProtobuf.JobId jobId = JobProtobuf.JobId.newBuilder().setId(jobsFlightTicket.getJobId()).build();
      final int offset = jobsFlightTicket.getOffset();
      final int limit = jobsFlightTicket.getLimit();

      try (final JobDataFragment jobDataFragment = jobsService.getJobData(JobsProtoUtil.toStuff(jobId), offset, limit)) {
        final Schema schema = jobDataFragment.getSchema();
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          serverStreamListener.start(root);
          for (RecordBatchHolder holder : jobDataFragment.getRecordBatches()) {
            // iterate over the columns
            for (int i = 0; i < schema.getFields().size(); i++) {
              ValueVector vector = root.getVector(schema.getFields().get(i).getName());
              ValueVector dataVector = holder.getData().getVectors().get(i);
              // iterate over values in the column to copy data
              for (int j = 0; j < dataVector.getValueCount(); j++ ) {
                vector.copyFromSafe(j, j, dataVector);
              }
              vector.setValueCount(dataVector.getValueCount());
              root.setRowCount(dataVector.getValueCount());
            }
            serverStreamListener.putNext();
            root.allocateNew();
          }
        }
        serverStreamListener.completed();
      }
    } catch (UserException ue) {
      serverStreamListener.error(JobsRpcUtils.toStatusRuntimeException(ue));
    } catch (Exception e) {
      serverStreamListener.error(Status.UNKNOWN.withCause(e).withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  //TODO (DX-19234): Implement me. Standard Flight procedure requires clients call getFlightInfo before getting stream
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }
}
