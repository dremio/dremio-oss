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
package com.dremio.service.execselector;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.Set;

/**
 * Select a set of executors that can perform a query of a given size
 *
 * <p>Thread safety note: - The caller will guarantee an exclusive lock around the calls to {@link
 * #nodesRegistered(Set)} and {@link #nodesUnregistered(Set)} - However, the caller will have a
 * non-exclusive lock around the call to {@link #getExecutors(int, ExecutorSelectionContext)} or the
 * call to {@link #getNumExecutors()} This means that while there might be several calls to {@link
 * #getExecutors(int, ExecutorSelectionContext)} running concurrently, these calls will never run
 * concurrently with calls to register or unregister a node
 */
public interface ExecutorSelector extends AutoCloseable {
  /**
   * Get the executor endpoints that can execute a query of size 'querySize'
   *
   * @param desiredNumExecutors Desired number of executors Note: this method is the more commonly
   *     used method among the three, and it needs to be optimized for speed. In particular, the
   *     code should not be making copies of the endpoints or the collection of endpoints at the
   *     time of this call
   * @param executorSelectionContext Contextual information for execution selection
   */
  ExecutorSelectionHandle getExecutors(
      int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext);

  /**
   * The action to taken when a set of nodes are unregistered from the cluster. The caller maintains
   * the guarantee that every unregistered node will be unregistered through this call. However, the
   * caller does *NOT* guarantee that the unregister event will be unique (i.e., there might be two
   * or more such events)
   *
   * @param unregisteredNodes the set of newly unregistered nodes.
   */
  void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes);

  /**
   * The action to taken when a set of new nodes are registered to the cluster. The caller maintains
   * the guarantee that every registered node will be registered through this call. However, the
   * caller does *NOT* guarantee that the register event will be unique (i.e., there might be two or
   * more such events). This is especially true during ExecutorSelector changes
   *
   * @param registeredNodes the set of newly registered nodes. Note: the complete set of currently
   *     registered nodes could be different.
   */
  void nodesRegistered(Set<NodeEndpoint> registeredNodes);

  /** Get the current number of executors registered with this selector */
  int getNumExecutors();
}
