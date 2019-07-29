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

import java.util.Collection;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

/**
 * Implementation of the {@link ExecutorSelectionHandle}
 */
public class ExecutorSelectionHandleImpl implements ExecutorSelectionHandle {
  private final Collection<NodeEndpoint> endpoints;

  public ExecutorSelectionHandleImpl(final Collection<NodeEndpoint> endpoints) {
    this.endpoints = endpoints;
  }

  @Override
  public Collection<NodeEndpoint> getExecutors() {
    return endpoints;
  }

  @Override
  public String getPlanDetails() {
    return "";
  }

  @Override
  public void close() {
    // Nothing to do
  }
}
