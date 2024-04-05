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

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.Collection;

/** Set with status listener. */
public interface ListenableSet {

  /**
   * Get a collection of available Sabot endpoints
   *
   * <p>The collection captures the available endpoints at a given time, which might be sligtly out
   * of date depending on the refresh policy of the provider.
   *
   * <p>Clients should account for this, and not make any assumption regarding the liveness of an
   * endpoint based on the returned value.
   *
   * @return A collection of available endpoints.
   */
  Collection<NodeEndpoint> getAvailableEndpoints();

  /**
   * Register a NodeStatusListener.
   *
   * <p>Note : the listeners are not guaranteed to be called in the order in which they call this
   * method.
   *
   * @param listener
   * @throws NullPointerException if listener is {@code null}
   */
  void addNodeStatusListener(NodeStatusListener listener);

  /**
   * Unregister a NodeStatusListener.
   *
   * @param listener
   * @throws NullPointerException if listener is {@code null}
   */
  void removeNodeStatusListener(NodeStatusListener listener);
}
