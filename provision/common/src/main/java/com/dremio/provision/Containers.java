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
package com.dremio.provision;

import java.util.List;

import org.immutables.value.Value.Immutable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Runtime info for a cluster
 */
@JsonDeserialize(builder = ImmutableContainers.Builder.class)
@Immutable
public interface Containers {
  List<Container> getRunningList();
  int getDecommissioningCount();
  int getProvisioningCount();
  int getPendingCount();
  List<Container> getDisconnectedList();
  List<Container> getPendingList();
  List<Container> getDecommissioningList();

  public static ImmutableContainers.Builder builder() {
    return new ImmutableContainers.Builder();
  }
}
