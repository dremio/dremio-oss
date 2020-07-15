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

import javax.inject.Provider;

import com.dremio.exec.catalog.CatalogInternalRPC.UpdateLastRefreshDateRequest;
import com.dremio.exec.store.CatalogService;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;


/**
 * CatalogServiceSynchronizer received a request from a coordinator after it performs a metadata refresh on a storage plugin
 * & updates last refresh date locally to avoid metadata refresh when it's unnecessary.
 */
public class CatalogServiceSynchronizer extends CatalogServiceSynchronizerGrpc.CatalogServiceSynchronizerImplBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CatalogServiceSynchronizer.class);

  private final Provider<CatalogService> catalogServiceProvider;

  public CatalogServiceSynchronizer(Provider<CatalogService> catalogServiceProvider) {
    this.catalogServiceProvider = catalogServiceProvider;
  }

  @Override
  public void updateRefreshDate(UpdateLastRefreshDateRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("Request received: {}", request);
    try {
      final ManagedStoragePlugin plugin = ((CatalogServiceImpl) catalogServiceProvider.get())
        .getManagedSource(request.getPluginName());
      plugin.setMetadataSyncInfo(request);
    } catch (Exception e) {
      responseObserver.onError(e);
      return;
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }
}
