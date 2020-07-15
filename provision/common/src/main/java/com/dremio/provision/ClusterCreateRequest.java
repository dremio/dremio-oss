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
import javax.validation.constraints.NotNull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Cluster creation request
 */
@JsonDeserialize(builder = ImmutableClusterCreateRequest.Builder.class)
@Immutable
@ConsistentProps.Annotation
public interface ClusterCreateRequest extends ConsistentProps {

  String getName();

  @NotNull ClusterType getClusterType();

  @NotNull DynamicConfig getDynamicConfig();

  YarnPropsApi getYarnProps();
  AwsPropsApi getAwsProps();
  boolean isAllowAutoStart();
  boolean isAllowAutoStop();
  @Default default long getShutdownInterval() { return 7_200_000; }

  public static ImmutableClusterCreateRequest.Builder builder() {
    return new ImmutableClusterCreateRequest.Builder();
  }
}
