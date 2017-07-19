/*
 * Copyright (C) 2017 Dremio Corporation
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
import java.util.concurrent.ConcurrentHashMap;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

/**
 * Base implementation for {@code ServiceSet}
 */
public abstract class AbstractServiceSet implements ServiceSet {
  private final ConcurrentHashMap<NodeStatusListener, NodeStatusListener> listeners = new ConcurrentHashMap<>(
      16, 0.75f, 16);

  protected AbstractServiceSet() {
  }

  /**
   * Actions to take when there are a set of new de-active nodes.
   * @param unregisteredNodes
   */
  protected void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
    for (NodeStatusListener listener : listeners.keySet()) {
      listener.nodesUnregistered(unregisteredNodes);
    }
  }

  protected void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    for (NodeStatusListener listener : listeners.keySet()) {
      listener.nodesRegistered(registeredNodes);
    }
  }

  @Override
  public void addNodeStatusListener(NodeStatusListener listener) {
    listeners.putIfAbsent(listener, listener);
  }

  @Override
  public void removeNodeStatusListener(NodeStatusListener listener) {
    listeners.remove(listener);
  }

  protected void clearListeners() {
    listeners.clear();
  }
}
