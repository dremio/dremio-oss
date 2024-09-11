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

import com.dremio.exec.catalog.CatalogInternalRPC.UpdateLastRefreshDateRequest;
import com.dremio.exec.catalog.CatalogServiceSynchronizerGrpc.CatalogServiceSynchronizerStub;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

/** Broadcasts the updated last metadata refresh date to all other sibling coordinators */
@Singleton
public class MetadataRefreshInfoBroadcaster {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MetadataRefreshInfoBroadcaster.class);
  private final Provider<ConduitProvider> conduitProviderProvider;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<NodeEndpoint> currentEndpointProvider;

  @Inject
  public MetadataRefreshInfoBroadcaster(
      Provider<ConduitProvider> conduitProvider,
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<NodeEndpoint> currentEndpoint) {
    this.conduitProviderProvider = conduitProvider;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.currentEndpointProvider = currentEndpoint;
  }

  public void communicateChange(UpdateLastRefreshDateRequest refreshRequest) {
    final Collection<NodeEndpoint> allCoordinators =
        clusterCoordinatorProvider.get().getCoordinatorEndpoints();
    final ConduitProvider conduitProvider = conduitProviderProvider.get();
    final NodeEndpoint currentEndpoint = currentEndpointProvider.get();

    for (NodeEndpoint nodeEndpoint : allCoordinators) {
      if (nodeEndpoint.equals(currentEndpoint)) {
        continue;
      }
      final ManagedChannel channel = conduitProvider.getOrCreateChannel(nodeEndpoint);
      final CatalogServiceSynchronizerStub stub = CatalogServiceSynchronizerGrpc.newStub(channel);
      stub.updateRefreshDate(
          refreshRequest,
          new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {}

            @Override
            public void onError(Throwable t) {
              logger.warn(
                  "Source '{}' error when CatalogServiceSynchronizer tried to update plugin's last refresh date.",
                  refreshRequest.getPluginName(),
                  t);
            }

            @Override
            public void onCompleted() {
              logger.debug(
                  "Source '{}' Successfully updated last refresh date in coordinator {}:{}.",
                  refreshRequest.getPluginName(),
                  nodeEndpoint.getAddress(),
                  nodeEndpoint.getUserPort());
            }
          });
    }
  }
}
