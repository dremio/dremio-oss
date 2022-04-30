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

package com.dremio.exec.planner.physical.filter;

import java.util.ArrayList;
import java.util.List;

import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("runtime-filter")
public class RuntimeFilterInfo {
  private final List<RuntimeFilterProbeTarget> runtimeFilterProbeTargets;
  private final boolean isBroadcastJoin;

  @JsonCreator
  public RuntimeFilterInfo(
      @JsonProperty("runtimeFilterProbeTargets")List<RuntimeFilterProbeTarget> runtimeFilterProbeTargets,
      @JsonProperty("broadcastJoin") boolean isBroadcastJoin) {
    this.runtimeFilterProbeTargets = runtimeFilterProbeTargets == null
      ? new ArrayList<>(0)
      : runtimeFilterProbeTargets;
    this.isBroadcastJoin = isBroadcastJoin;
  }

  public List<RuntimeFilterProbeTarget> getRuntimeFilterProbeTargets() {
    return runtimeFilterProbeTargets;
  }

  public boolean isBroadcastJoin() {
    return isBroadcastJoin;
  }

  @Override
  public String toString() {
    if (runtimeFilterProbeTargets.isEmpty()) {
      return "";
    }
    return runtimeFilterProbeTargets.toString();
  }


  public static class Builder {
    private List<RuntimeFilterProbeTarget> runtimeFilterProbeTargets;
    private boolean isBroadcastJoin;

    public Builder() {
    }

    public Builder setRuntimeFilterProbeTargets(
      List<RuntimeFilterProbeTarget> runtimeFilterProbeTargets) {
      this.runtimeFilterProbeTargets = runtimeFilterProbeTargets;
      return this;
    }

    public Builder isBroadcastJoin (boolean isBroadcastJoin) {
      this.isBroadcastJoin = isBroadcastJoin;
      return this;
    }

    public RuntimeFilterInfo build() {
      return new RuntimeFilterInfo(runtimeFilterProbeTargets, isBroadcastJoin);
    }

  }
}
