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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Cluster response
 */
@JsonDeserialize(builder = ImmutableClusterResponse.Builder.class)
@Immutable
@Valid
public interface ClusterResponse {

  @NotNull
  String getId();

  String getTag();

  @NotNull
  ClusterState getCurrentState();

  Long getStateChangeTime();

  @NotNull
  ClusterType getClusterType();

  String getName();

  @NotNull
  DynamicConfig getDynamicConfig();

  Containers getContainers();

  String getError();
  String getDetailedError();
  ClusterDesiredState getDesiredState();

  YarnPropsApi getYarnProps();

  AwsPropsApi getAwsProps();

  boolean isAllowAutoStart();
  boolean isAllowAutoStop();
  @Default default long getShutdownInterval() { return 900_000; }

  public static ImmutableClusterResponse.Builder builder() {
    return new ImmutableClusterResponse.Builder();
  }

}
