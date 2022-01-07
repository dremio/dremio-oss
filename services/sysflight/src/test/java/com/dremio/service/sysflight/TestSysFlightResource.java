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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.rules.ExternalResource;

import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ActiveJobsRequest;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.sysflight.SystemTableManager.TABLES;
import com.google.common.collect.Maps;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * Sys Flight test resource
 */
public class TestSysFlightResource extends ExternalResource {

  private Server server;
  private ManagedChannel channel;

  // mock ChronicleGrpc service
  private final ChronicleGrpc.ChronicleImplBase chronicleService =
    mock(ChronicleGrpc.ChronicleImplBase.class,
       delegatesTo(
         new ChronicleGrpc.ChronicleImplBase(){
           @Override
           public void getActiveJobs(ActiveJobsRequest jobsRequest,
                                     io.grpc.stub.StreamObserver<com.dremio.service.job.ActiveJobSummary> responseObserver){
             responseObserver.onNext(ActiveJobSummary.newBuilder()
               .setJobId("1")
               .setStatus("RUNNING")
               .setQueryType("UI_RUN")
               .setUserName("user")
               .setAccelerated(false)
               .setErrorMsg("err")
               .build());
             responseObserver.onNext(ActiveJobSummary.getDefaultInstance());
             responseObserver.onCompleted();
           }
         }
       ));

  @Override
  public void before() throws Exception {
    setupServer();
  }

  /** instantiates in-process gRPC server with mock implementation & creates a channel to it */
  private void setupServer() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(serverName).directExecutor().addService(chronicleService).build().start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  private ChronicleGrpc.ChronicleStub getChronicleStub(){
    return  ChronicleGrpc.newStub(channel);
  }

  public Map<TABLES, SysFlightDataProvider> getTablesProvider() {
    Map<TABLES, SysFlightDataProvider> tablesMap = Maps.newHashMap();
    tablesMap.put(TABLES.JOBS, new JobsTableProvider(getChronicleStub()));
    return tablesMap;
  }

  @Override
  public void after() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  private static class JobsTableProvider implements SysFlightDataProvider {
    private final ChronicleGrpc.ChronicleStub jobsStub;

    JobsTableProvider(ChronicleGrpc.ChronicleStub jobsStub) {
      this.jobsStub = jobsStub;
    }

    @Override
    public void streamData(
      SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator, int recordBatchSize) {
      ActiveJobsRequest searchJobsRequest = ActiveJobsRequest.newBuilder().setQuery(ticket.getQuery()).build();
      jobsStub.getActiveJobs(searchJobsRequest, new SysFlightStreamObserver<>(allocator, listener,
        ActiveJobSummary.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(ActiveJobSummary.getDescriptor());
    }
  }
}
