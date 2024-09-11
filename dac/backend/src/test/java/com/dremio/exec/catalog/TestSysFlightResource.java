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
package com.dremio.exec.catalog;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import com.dremio.dac.service.sysflight.SysFlightTablesProvider.JobsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.MaterializationsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.RecentJobsTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionDependenciesTable;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionLineageTableFunction;
import com.dremio.dac.service.sysflight.SysFlightTablesProvider.ReflectionsTable;
import com.dremio.exec.server.SabotNode;
import com.dremio.exec.store.CatalogService;
import com.dremio.plugins.sysflight.SysFlightPluginConf;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceImplBase;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListMaterializationsRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListMaterializationsResponse;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionDependenciesRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionLineageRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionLineageResponse;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionsRequest;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC.ListReflectionsResponse;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ActiveJobsRequest;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.RecentJobSummary;
import com.dremio.service.job.RecentJobsRequest;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.sysflight.SysFlightDataProvider;
import com.dremio.service.sysflight.SystemTableManager.TABLES;
import com.dremio.service.sysflight.SystemTableManager.TABLE_FUNCTIONS;
import com.dremio.service.sysflight.SystemTables;
import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import org.junit.rules.ExternalResource;

/** Sys Flight test resource */
public class TestSysFlightResource extends ExternalResource {

  private Server server;
  private ManagedChannel channel;

  // mock ChronicleGrpc service
  private final ChronicleGrpc.ChronicleImplBase chronicleService =
      mock(
          ChronicleGrpc.ChronicleImplBase.class,
          delegatesTo(
              new ChronicleGrpc.ChronicleImplBase() {
                @Override
                public void getActiveJobs(
                    ActiveJobsRequest jobsRequest,
                    io.grpc.stub.StreamObserver<com.dremio.service.job.ActiveJobSummary>
                        responseObserver) {
                  responseObserver.onNext(
                      ActiveJobSummary.newBuilder()
                          .setJobId("1")
                          .setStatus("RUNNING")
                          .setQueryType("UI_RUN")
                          .setUserName("user")
                          .setAccelerated(false)
                          .setErrorMsg("err")
                          .setIsProfileIncomplete(true)
                          .setExecutionAllocatedBytes(1000)
                          .setExecutionCpuTimeMillis(10)
                          .build());
                  responseObserver.onNext(ActiveJobSummary.getDefaultInstance());
                  responseObserver.onCompleted();
                }

                @Override
                public void getRecentJobs(
                    RecentJobsRequest recentJobsRequest,
                    io.grpc.stub.StreamObserver<com.dremio.service.job.RecentJobSummary>
                        responseObserver) {
                  responseObserver.onNext(
                      RecentJobSummary.newBuilder()
                          .setJobId("2")
                          .setStatus("RUNNING")
                          .setQueryType("UI_RUN")
                          .setUserName("user")
                          .setAccelerated(true)
                          .setErrorMsg("errmsg")
                          .setIsProfileIncomplete(true)
                          .setExecutionAllocatedBytes(2000)
                          .setExecutionCpuTimeMillis(20)
                          .build());
                  responseObserver.onNext(RecentJobSummary.getDefaultInstance());
                  responseObserver.onCompleted();
                }
              }));

  private final ReflectionDescriptionServiceImplBase acclService =
      mock(
          ReflectionDescriptionServiceImplBase.class,
          delegatesTo(
              new ReflectionDescriptionServiceImplBase() {
                @Override
                public void listReflections(
                    ListReflectionsRequest request,
                    StreamObserver<ListReflectionsResponse> responseObserver) {
                  responseObserver.onNext(
                      ListReflectionsResponse.newBuilder()
                          .setDatasetId("datasetId")
                          .setDatasetName("datasetName")
                          .setReflectionId("reflectionId")
                          .build());
                  responseObserver.onNext(ListReflectionsResponse.getDefaultInstance());
                  responseObserver.onCompleted();
                }

                @Override
                public void listReflectionDependencies(
                    ListReflectionDependenciesRequest request,
                    StreamObserver<ListReflectionDependenciesResponse> responseObserver) {
                  ListReflectionDependenciesResponse resp =
                      ListReflectionDependenciesResponse.getDefaultInstance();
                  responseObserver.onNext(resp);
                  responseObserver.onCompleted();
                }

                @Override
                public void listMaterializations(
                    ListMaterializationsRequest request,
                    StreamObserver<ListMaterializationsResponse> responseObserver) {
                  ListMaterializationsResponse resp =
                      ListMaterializationsResponse.getDefaultInstance();
                  responseObserver.onNext(resp);
                  responseObserver.onCompleted();
                }

                @Override
                public void listReflectionLineage(
                    ListReflectionLineageRequest request,
                    StreamObserver<ListReflectionLineageResponse> responseObserver) {
                  responseObserver.onNext(
                      ListReflectionLineageResponse.newBuilder()
                          .setBatchNumber(1)
                          .setReflectionId("reflectionId")
                          .setReflectionName("reflectionName")
                          .setDatasetName("datasetName")
                          .build());
                  responseObserver.onNext(ListReflectionLineageResponse.getDefaultInstance());
                  responseObserver.onCompleted();
                }
              }));

  @Override
  public void before() throws Exception {
    setupServer();
  }

  /** instantiates in-process gRPC server with mock implementation & creates a channel to it */
  private void setupServer() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(chronicleService)
            .addService(acclService)
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
  }

  private ChronicleGrpc.ChronicleStub getChronicleStub() {
    return ChronicleGrpc.newStub(channel);
  }

  private ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceStub getReflectionStub() {
    return ReflectionDescriptionServiceGrpc.newStub(channel);
  }

  Map<SystemTables, SysFlightDataProvider> getTablesProvider() {
    Map<SystemTables, SysFlightDataProvider> tablesMap = Maps.newHashMap();
    tablesMap.put(TABLES.JOBS, new JobsTable(this::getChronicleStub));
    tablesMap.put(TABLES.REFLECTIONS, new ReflectionsTable(this::getReflectionStub));
    tablesMap.put(TABLES.MATERIALIZATIONS, new MaterializationsTable(this::getReflectionStub));
    tablesMap.put(
        TABLES.REFLECTION_DEPENDENCIES, new ReflectionDependenciesTable(this::getReflectionStub));
    tablesMap.put(TABLES.JOBS_RECENT, new RecentJobsTable(this::getChronicleStub));
    return tablesMap;
  }

  Map<TABLE_FUNCTIONS, SysFlightDataProvider> getTableFunctionsProvider() {
    Map<TABLE_FUNCTIONS, SysFlightDataProvider> tableFunctionsMap = Maps.newHashMap();
    tableFunctionsMap.put(
        TABLE_FUNCTIONS.REFLECTION_LINEAGE,
        new ReflectionLineageTableFunction(this::getReflectionStub));
    return tableFunctionsMap;
  }

  @Override
  public void after() {
    channel.shutdownNow();
    server.shutdownNow();
  }

  static void addSysFlightPlugin(SabotNode node) {
    // create the sysFlight source
    SourceConfig c = new SourceConfig();
    SysFlightPluginConf conf = new SysFlightPluginConf();
    c.setType(conf.getType());
    c.setName("sys");
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    c.setConfig(conf.toBytesString());

    node.getContext().getCatalogService().getSystemUserCatalog().createSource(c);
  }
}
