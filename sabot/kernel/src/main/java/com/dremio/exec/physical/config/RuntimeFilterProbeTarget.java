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
package com.dremio.exec.physical.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/** Groups the information for runtime filtering. */
public class RuntimeFilterProbeTarget {
  private final int probeScanMajorFragmentId;
  private final int probeScanOperatorId;

  // partitioned at the probe side, and respective build side keys
  private final List<String> partitionBuildTableKeys;
  private final List<String> partitionProbeTableKeys;

  // non-partitioned at the probe side, and respective build side keys
  private final List<String> nonPartitionBuildTableKeys;
  private final List<String> nonPartitionProbeTableKeys;

  @JsonCreator
  public RuntimeFilterProbeTarget(
      @JsonProperty("probeScanMajorFragmentId") int probeScanMajorFragmentId,
      @JsonProperty("probeScanOperatorId") int probeScanOperatorId,
      @JsonProperty("partitionBuildTableKeys") List<String> partitionBuildTableKeys,
      @JsonProperty("partitionProbeTableKeys") List<String> partitionProbeTableKeys,
      @JsonProperty("nonPartitionBuildTableKeys") List<String> nonPartitionBuildTableKeys,
      @JsonProperty("nonPartitionProbeTableKeys") List<String> nonPartitionProbeTableKeys) {
    this.probeScanMajorFragmentId = probeScanMajorFragmentId;
    this.probeScanOperatorId = probeScanOperatorId;
    this.partitionBuildTableKeys = partitionBuildTableKeys;
    this.partitionProbeTableKeys = partitionProbeTableKeys;
    this.nonPartitionBuildTableKeys = nonPartitionBuildTableKeys;
    this.nonPartitionProbeTableKeys = nonPartitionProbeTableKeys;
  }

  public boolean isSameProbeCoordinate(int majorFragmentId, int operatorId) {
    return this.probeScanMajorFragmentId == majorFragmentId
        && this.probeScanOperatorId == operatorId;
  }

  public List<String> getPartitionBuildTableKeys() {
    return partitionBuildTableKeys;
  }

  public List<String> getPartitionProbeTableKeys() {
    return partitionProbeTableKeys;
  }

  public List<String> getNonPartitionBuildTableKeys() {
    return nonPartitionBuildTableKeys;
  }

  public List<String> getNonPartitionProbeTableKeys() {
    return nonPartitionProbeTableKeys;
  }

  public int getProbeScanMajorFragmentId() {
    return probeScanMajorFragmentId;
  }

  public int getProbeScanOperatorId() {
    return probeScanOperatorId;
  }

  @Override
  public String toString() {
    return "RuntimeFilterInfoProbeTarget{"
        + "probeScanMajorFragmentId="
        + probeScanMajorFragmentId
        + ", probeScanOperatorId="
        + probeScanOperatorId
        + ", partitionBuildTableKeys="
        + partitionBuildTableKeys
        + ", partitionProbeTableKeys="
        + partitionProbeTableKeys
        + ", nonPartitionBuildTableKeys="
        + nonPartitionBuildTableKeys
        + ", nonPartitionProbeTableKeys="
        + nonPartitionProbeTableKeys
        + '}';
  }

  public String toTargetIdString() {
    return "RuntimeFilterInfoProbeTarget{"
        + "probeScanMajorFragmentId="
        + probeScanMajorFragmentId
        + ", probeScanOperatorId="
        + probeScanOperatorId
        + '}';
  }

  @NotThreadSafe
  public static class Builder {
    private final int probeScanMajorFragmentId;
    private final int probeScanOperatorId;

    // partitioned at the probe side, and respective build side keys
    private final ImmutableList.Builder<String> partitionBuildTableKeys = ImmutableList.builder();
    private final ImmutableList.Builder<String> partitionProbeTableKeys = ImmutableList.builder();

    // non-partitioned at the probe side, and respective build side keys
    private final ImmutableList.Builder<String> nonPartitionBuildTableKeys =
        ImmutableList.builder();
    private final ImmutableList.Builder<String> nonPartitionProbeTableKeys =
        ImmutableList.builder();

    public Builder(int probeScanMajorFragmentId, int probeScanOperatorId) {
      this.probeScanMajorFragmentId = probeScanMajorFragmentId;
      this.probeScanOperatorId = probeScanOperatorId;
    }

    public Builder addPartitionKey(String buildTableKey, String probeTableKey) {
      partitionBuildTableKeys.add(buildTableKey);
      partitionProbeTableKeys.add(probeTableKey);
      return this;
    }

    public Builder addNonPartitionKey(String buildTableKey, String probeTableKey) {
      nonPartitionBuildTableKeys.add(buildTableKey);
      nonPartitionProbeTableKeys.add(probeTableKey);
      return this;
    }

    public RuntimeFilterProbeTarget build() {
      return new RuntimeFilterProbeTarget(
          probeScanMajorFragmentId,
          probeScanOperatorId,
          partitionBuildTableKeys.build(),
          partitionProbeTableKeys.build(),
          nonPartitionBuildTableKeys.build(),
          nonPartitionProbeTableKeys.build());
    }
  }
}
