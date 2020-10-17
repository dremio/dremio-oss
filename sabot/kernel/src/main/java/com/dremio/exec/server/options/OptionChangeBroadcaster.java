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
package com.dremio.exec.server.options;

import java.util.Collection;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.options.OptionChangeNotification;
import com.dremio.options.OptionNotificationServiceGrpc;
import com.dremio.service.conduit.client.ConduitProvider;
import com.google.protobuf.Empty;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * Broadcasts the request to refetch system options from kv store to all other sibling coordinators
 */
public class OptionChangeBroadcaster {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionChangeBroadcaster.class);
  private final Provider<ConduitProvider> conduitProvider;
  private final Provider<Collection<NodeEndpoint>> coordinatorEndpoints;
  private final Provider<NodeEndpoint> currentEndpoint;

  public OptionChangeBroadcaster(
    Provider<ConduitProvider> conduitProvider,
    Provider<Collection<NodeEndpoint>> coordinatorEndpoints,
    Provider<NodeEndpoint> currentEndpoint
  ) {
    this.conduitProvider = conduitProvider;
    this.coordinatorEndpoints = coordinatorEndpoints;
    this.currentEndpoint = currentEndpoint;
  }

  public void communicateChange(OptionChangeNotification fetchRequest) {
    final Collection<NodeEndpoint> allCoordinators = this.coordinatorEndpoints.get();
    final ConduitProvider conduitProvider = this.conduitProvider.get();
    final NodeEndpoint currentEndpoint = this.currentEndpoint.get();

    for (NodeEndpoint nodeEndpoint: allCoordinators) {
      if (nodeEndpoint.equals(currentEndpoint)) {
        continue;
      }
      final ManagedChannel channel = conduitProvider.getOrCreateChannel(nodeEndpoint);
      final OptionNotificationServiceGrpc.OptionNotificationServiceStub stub = OptionNotificationServiceGrpc.newStub(channel);
      stub.systemOptionFetch(fetchRequest, new StreamObserver<Empty>() {
        @Override
        public void onNext(Empty empty) {}

        @Override
        public void onError(Throwable t) {
          logger.warn("Error when OptionNotificationService fetches system options from kv store per broadcast in coordinator {}:{}.",
            nodeEndpoint.getAddress(), nodeEndpoint.getUserPort());
        }

        @Override
        public void onCompleted() {
          logger.debug("Successfully fetch system option per broadcast in coordinator {}:{}.",
            nodeEndpoint.getAddress(), nodeEndpoint.getUserPort());
        }
      });
    }
  }
}
