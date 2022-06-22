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

import java.util.Iterator;
import java.util.concurrent.Executor;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC;
import com.dremio.service.grpc.OnReadyHandler;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.collect.Streams;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 *Acceleration List Service gRPC implements {@link ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceImplBase}
 */

public class AccelerationListServiceImpl extends ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceImplBase {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationListServiceImpl.class);


  private final Provider<ReflectionStatusService> reflectionStatusService;
  private final Provider<com.dremio.service.reflection.ReflectionService> reflectionService;
  private final MaterializationStore materializationStore;
  private final Provider<Executor> executor;

  public AccelerationListServiceImpl (
    Provider<ReflectionStatusService> reflectionStatusService,
    Provider<com.dremio.service.reflection.ReflectionService> reflectionService,
    Provider<LegacyKVStoreProvider> storeProvider,
    Provider<Executor> executor
  ) {
    this.reflectionStatusService = reflectionStatusService;
    this.reflectionService = reflectionService;
    this.materializationStore = new MaterializationStore(storeProvider);
    this.executor = executor;
  }

  private com.dremio.service.reflection.ReflectionService getReflectionService(){
    return  this.reflectionService.get();
  }

  private ReflectionStatusService getReflectionStatusService(){
    return this.reflectionStatusService.get();
  }

  @Override
  public void listReflections(ReflectionDescriptionServiceRPC.ListReflectionsRequest request,
                              StreamObserver<ReflectionDescriptionServiceRPC.ListReflectionsResponse> responseObserver) {
    Iterator<AccelerationListManager.ReflectionInfo> reflections = getReflectionStatusService().getReflections();
    Iterator<ReflectionDescriptionServiceRPC.ListReflectionsResponse> reflectionsProto = Streams.stream(reflections).map(AccelerationListManager.ReflectionInfo::toProto).iterator();

    final ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListReflectionsResponse> streamObserver = (ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListReflectionsResponse>) responseObserver;

    final class Reflections extends OnReadyHandler<ReflectionDescriptionServiceRPC.ListReflectionsResponse>{
      Reflections(){
        super("get-reflections", AccelerationListServiceImpl.this.executor.get(), streamObserver, reflectionsProto);
      }
    }

    Reflections reflectionsStream = new Reflections();
    streamObserver.setOnReadyHandler(reflectionsStream);
    streamObserver.setOnCancelHandler(reflectionsStream::cancel);
  }

  @Override
  public void getRefreshInfo(ReflectionDescriptionServiceRPC.GetRefreshInfoRequest request,
                             StreamObserver<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse> responseObserver) {
    Iterator<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse> refreshInfos = getReflectionStatusService().getRefreshInfos();

    final ServerCallStreamObserver<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse> streamObserver = (ServerCallStreamObserver<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse>) responseObserver;

    final class RefreshInfo extends OnReadyHandler<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse>{
      RefreshInfo(){
        super("get-refresh-info", AccelerationListServiceImpl.this.executor.get(), streamObserver, refreshInfos);
      }
    }

    RefreshInfo refreshInfo = new RefreshInfo();
    streamObserver.setOnReadyHandler(refreshInfo);
    streamObserver.setOnCancelHandler(refreshInfo::cancel);
  }

  @Override
  public void listReflectionDependencies(ReflectionDescriptionServiceRPC.ListReflectionDependenciesRequest request,
                                        StreamObserver<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse> responseObserver) {
    logger.info("Received listReflectionDependencies request {}", request);
    Iterator<AccelerationListManager.DependencyInfo> dependencyInfos = getReflectionService().getReflectionDependencies();
    Iterator<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse> dependenciesProto = Streams.stream(dependencyInfos).map(AccelerationListManager.DependencyInfo::toProto).iterator();

    final ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse> streamObserver = (ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse>) responseObserver;

    final class ReflectionDependencies extends OnReadyHandler<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse>{
      ReflectionDependencies(){
        super("get-reflection-dependencies", AccelerationListServiceImpl.this.executor.get(), streamObserver, dependenciesProto);
      }
    }

    final ReflectionDependencies reflectionDependencies = new ReflectionDependencies();
    streamObserver.setOnReadyHandler(reflectionDependencies);
    streamObserver.setOnCancelHandler(reflectionDependencies::cancel);
  }

  @Override
  public void listMaterializations(ReflectionDescriptionServiceRPC.ListMaterializationsRequest request,
                                  StreamObserver<ReflectionDescriptionServiceRPC.ListMaterializationsResponse> responseObserver) {
    Iterator<AccelerationListManager.MaterializationInfo> materializationInfos =
      AccelerationMaterializationUtils.getMaterializationsFromStore(materializationStore);
    Iterator<ReflectionDescriptionServiceRPC.ListMaterializationsResponse> materializationProto = Streams.stream(materializationInfos).map(AccelerationListManager.MaterializationInfo::toProto).iterator();

    final ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListMaterializationsResponse> streamObserver = (ServerCallStreamObserver<ReflectionDescriptionServiceRPC.ListMaterializationsResponse>) responseObserver;

    final class Materializations extends OnReadyHandler<ReflectionDescriptionServiceRPC.ListMaterializationsResponse> {
      Materializations(){
        super("get-materializations", AccelerationListServiceImpl.this.executor.get(), streamObserver, materializationProto);
      }
    }

    final Materializations materializations = new Materializations();
    streamObserver.setOnReadyHandler(materializations);
    streamObserver.setOnCancelHandler(materializations::cancel);
  }
}
