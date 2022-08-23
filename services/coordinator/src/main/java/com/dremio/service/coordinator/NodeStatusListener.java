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
package com.dremio.service.coordinator;

import java.util.Set;

import com.dremio.exec.proto.CoordinationProtos;

/**
 * Interface to define the listener to take actions when the set of active nodes is changed.
 */
public interface NodeStatusListener {

  /**
   * The action to taken when a set of nodes are unregistered from the cluster.
   * @param  unregisteredNodes the set of newly unregistered nodes.
   */
  void nodesUnregistered(Set<CoordinationProtos.NodeEndpoint> unregisteredNodes);

  /**
   * The action to taken when a set of new nodes are registered to the cluster.
   * @param  registeredNodes the set of newly registered nodes. Note: the complete set of currently registered nodes could be different.
   */
  void nodesRegistered(Set<CoordinationProtos.NodeEndpoint> registeredNodes);

}
