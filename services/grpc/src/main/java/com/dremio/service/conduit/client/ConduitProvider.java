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

package com.dremio.service.conduit.client;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

import io.grpc.ManagedChannel;

/**
 * Provides {@link ManagedChannel managed channels} to other nodes in the cluster, reconnecting if necessary.
 */
public interface ConduitProvider {

  /**
   * Get or create an active (best effort) managed channel to the given peer endpoint.
   *
   * @param peerEndpoint peer endpoint
   * @return managed channel
   */
  ManagedChannel getOrCreateChannel(NodeEndpoint peerEndpoint);

  /**
   * Get or create an active (best effort) managed channel to the master coordinator.
   *
   * @return managed channel
   */
  ManagedChannel getOrCreateChannelToMaster();

}
