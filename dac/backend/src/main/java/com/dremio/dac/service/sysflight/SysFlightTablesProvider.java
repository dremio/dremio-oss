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
package com.dremio.dac.service.sysflight;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListMaterializationsRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListMaterializationsResponse;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionDependenciesRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionsRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionsResponse;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ActiveJobsRequest;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.RecentJobSummary;
import com.dremio.service.job.RecentJobsRequest;
import com.dremio.service.sysflight.ProtobufRecordReader;
import com.dremio.service.sysflight.SysFlightDataProvider;
import com.dremio.service.sysflight.SysFlightStreamObserver;

/**
 * SysFlight tables provider
 */
public class SysFlightTablesProvider {

  /**
   * Jobs table
   */
  public static class JobsTable implements SysFlightDataProvider {
    private final Provider<ChronicleGrpc.ChronicleStub> jobsStub;
    public JobsTable(Provider<ChronicleGrpc.ChronicleStub> jobsStub) {
      this.jobsStub = jobsStub;
    }

    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator,
      int recordBatchSize) {
      ActiveJobsRequest.Builder requestBuilder = ActiveJobsRequest.newBuilder().setQuery(ticket.getQuery());
      if(!(ticket.getUserName().equals(""))){
        requestBuilder.setUserName(ticket.getUserName());
      }
      jobsStub.get().getActiveJobs(requestBuilder.build(), new SysFlightStreamObserver<>(allocator, listener,
        ActiveJobSummary.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(ActiveJobSummary.getDescriptor());
    }
  }


  /**
   * Reflections table
   */
  public static class ReflectionsTable implements SysFlightDataProvider {
    private final Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub;
    public ReflectionsTable(
      Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub) {
      this.reflectionsStub = reflectionsStub;
    }

    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator,
      int recordBatchSize) {
      ListReflectionsRequest request = ListReflectionsRequest.newBuilder().build();
      reflectionsStub.get().listReflections(request, new SysFlightStreamObserver<>(allocator, listener,
        ListReflectionsResponse.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(ListReflectionsResponse.getDescriptor());
    }
  }

  /**
   * Materializations table
   */
  public static class MaterializationsTable implements SysFlightDataProvider {
    private final Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub;
    public MaterializationsTable(
      Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub) {
      this.reflectionsStub = reflectionsStub;
    }

    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator,
      int recordBatchSize) {
      ListMaterializationsRequest request = ListMaterializationsRequest.newBuilder().build();
      reflectionsStub.get().listMaterializations(request, new SysFlightStreamObserver<>(allocator, listener,
        ListMaterializationsResponse.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(ListMaterializationsResponse.getDescriptor());
    }
  }

  /**
   * Reflection Dependencies table
   */
  public static class ReflectionDependenciesTable implements SysFlightDataProvider {
    private final Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub;
    public ReflectionDependenciesTable(
      Provider<ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub> reflectionsStub) {
      this.reflectionsStub = reflectionsStub;
    }

    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator,
      int recordBatchSize) {
      ListReflectionDependenciesRequest request = ListReflectionDependenciesRequest.newBuilder().build();
      reflectionsStub.get().listReflectionDependencies(request, new SysFlightStreamObserver<>(allocator, listener,
        ListReflectionDependenciesResponse.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(ListReflectionDependenciesResponse.getDescriptor());
    }
  }

  /**
   * Recent Jobs table
   */
  public static class RecentJobsTable implements SysFlightDataProvider {
    private final Provider<ChronicleGrpc.ChronicleStub> jobsStub;
    public RecentJobsTable(Provider<ChronicleGrpc.ChronicleStub> jobsStub) {
      this.jobsStub = jobsStub;
    }

    @Override
    public void streamData(SysFlightTicket ticket, ServerStreamListener listener, BufferAllocator allocator,
                           int recordBatchSize) {
      RecentJobsRequest.Builder requestBuilder = RecentJobsRequest.newBuilder().setQuery(ticket.getQuery());
      if(!(ticket.getUserName().equals(""))){
        requestBuilder.setUserName(ticket.getUserName());
      }
      jobsStub.get().getRecentJobs(requestBuilder.build(), new SysFlightStreamObserver<>(allocator, listener,
        RecentJobSummary.getDescriptor(), recordBatchSize));
    }

    @Override
    public BatchSchema getSchema() {
      return ProtobufRecordReader.getSchema(RecentJobSummary.getDescriptor());
    }
  }
}
