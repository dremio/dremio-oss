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

package com.dremio.service.sysflight;

import java.util.ArrayList;
import java.util.Iterator;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ActiveJobsRequest;
import com.dremio.service.job.ChronicleGrpc;
import com.google.common.annotations.VisibleForTesting;

/**
 * Manages the system tables.
 */
class SystemTableManager implements AutoCloseable{
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SystemTableManager.class);

  public enum TABLES {
    JOBS("jobs");

    private final String name;
    TABLES(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    public static TABLES fromString(String input) {
      for (TABLES t : TABLES.values()) {
        if (t.name.equalsIgnoreCase(input)) {
          return t;
        }
      }
      throwUnsupportedException(input);
      return null;
    }
  }

  private final BufferAllocator allocator;
  private final Provider<ChronicleGrpc.ChronicleBlockingStub> jobsStub;

  private int recordBatchSize;

  SystemTableManager(BufferAllocator allocator,
                     Provider<ChronicleGrpc.ChronicleBlockingStub> jobsStub) {
    this.allocator = allocator;
    this.recordBatchSize = 4000;
    this.jobsStub = jobsStub;
  }

  void streamData(SysFlightTicket ticket, ServerStreamListener listener) {
    switch (TABLES.fromString(ticket.getDatasetName())) {
      case JOBS:{
        ActiveJobsRequest searchJobsRequest = ActiveJobsRequest.newBuilder().build();
        Iterator<ActiveJobSummary> jobs = jobsStub.get().getActiveJobs(searchJobsRequest);

        ProtobufRecordReader.streamData(allocator, jobs, ActiveJobSummary.getDescriptor(),listener, recordBatchSize);
        return;
      }

      default:
        throwUnsupportedException(ticket.getDatasetName());
    }
  }

  void listSchemas(StreamListener<FlightInfo> listener) {
    for(TABLES t : TABLES.values()) {
      FlightInfo info = new FlightInfo(getSchema(t.getName()), FlightDescriptor.path(t.getName()),
        new ArrayList<>(), -1, -1);
      listener.onNext(info);
    }
    listener.onCompleted();
  }

  Schema getSchema(String datasetName) {
    switch (TABLES.fromString(datasetName)) {
      case JOBS:{
        return ProtobufRecordReader.getSchema(ActiveJobSummary.getDescriptor());
      }

      default:
        throwUnsupportedException(datasetName);
    }
    return null;
  }

  private static void throwUnsupportedException(String datasetName) {
    throw UserException.unsupportedError()
      .message("'%s' system table is not supported.", datasetName)
      .buildSilently();
  }

  @VisibleForTesting
  public void setRecordBatchSize(int recordBatchSize) {
    this.recordBatchSize = recordBatchSize;
  }

  @VisibleForTesting
  public int getRecordBatchSize() {
    return recordBatchSize;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }
}
