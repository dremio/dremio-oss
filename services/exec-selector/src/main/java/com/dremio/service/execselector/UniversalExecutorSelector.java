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

import java.util.HashSet;
import java.util.Set;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.collect.ImmutableSet;

/**
 * An executor selector that always returns the full set of nodes, regardless of the query size
 */
public class UniversalExecutorSelector implements ExecutorSelector {
  static final String EXECUTOR_SELECTOR_TYPE = "universal";

  private Set<NodeEndpoint> endpoints = new HashSet<>();

  @Override
  public ExecutorSelectionHandle getExecutors(int desiredNumExecutors, ExecutorSelectionContext executorSelectionContext) {
    return new ExecutorSelectionHandleImpl(ImmutableSet.copyOf(endpoints));
  }

  @Override
  public void nodesUnregistered(Set<NodeEndpoint> unregisteredNodes) {
    // NB: w-lock held in caller. Safe to directly manipulate 'endpoints'
    endpoints.removeAll(unregisteredNodes);
  }

  @Override
  public void nodesRegistered(Set<NodeEndpoint> registeredNodes) {
    // NB: w-lock held in caller. Safe to directly manipulate 'endpoints'
    endpoints.addAll(registeredNodes);
  }

  @Override
  public int getNumExecutors() {
    return endpoints.size();
  }

  @Override
  public void close() {
  }
}
