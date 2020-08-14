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
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("runtime-filter")
public class RuntimeFilterInfo {
  private List<RuntimeFilterEntry> nonPartitionJoinColumns;
  private List<RuntimeFilterEntry> partitionJoinColumns;
  private boolean isBroadcastJoin;

  @JsonCreator
  public RuntimeFilterInfo(@JsonProperty("nonPartitionRFEntry")List<RuntimeFilterEntry> nonPartitionJoinColumns,
                           @JsonProperty("partitionRFEntry")List<RuntimeFilterEntry> partitionJoinColumns,
                           @JsonProperty("isBroadcastJoin") boolean isBroadcastJoin) {
    this.nonPartitionJoinColumns = Optional.ofNullable(nonPartitionJoinColumns).orElse(new ArrayList<>(0));
    this.partitionJoinColumns = Optional.ofNullable(partitionJoinColumns).orElse(new ArrayList<>(0));
    this.isBroadcastJoin = isBroadcastJoin;
  }

  public static class Builder {
    private List<RuntimeFilterEntry> nonPartitionJoinColumns;
    private List<RuntimeFilterEntry> partitionJoinColumns;
    private boolean isBroadcastJoin;

    public Builder() {
    }

    public Builder nonPartitionJoinColumns (List<RuntimeFilterEntry> nonPartitionJoinColumns) {
      this.nonPartitionJoinColumns = nonPartitionJoinColumns;
      return this;
    }

    public Builder partitionJoinColumns (List<RuntimeFilterEntry> partitionJoinColumns) {
      this.partitionJoinColumns = partitionJoinColumns;
      return this;
    }

    public Builder isBroadcastJoin (boolean isBroadcastJoin) {
      this.isBroadcastJoin = isBroadcastJoin;
      return this;
    }

    public RuntimeFilterInfo build() {
      return new RuntimeFilterInfo(this);
    }

  }

  public RuntimeFilterInfo(Builder builder) {
    this.nonPartitionJoinColumns = Optional.ofNullable(builder.nonPartitionJoinColumns).orElse(new ArrayList<>(0));
    this.partitionJoinColumns = Optional.ofNullable(builder.partitionJoinColumns).orElse(new ArrayList<>(0));
    this.isBroadcastJoin = builder.isBroadcastJoin;
  }

  public List<RuntimeFilterEntry> getNonPartitionJoinColumns() {
    return nonPartitionJoinColumns;
  }

  public void setNonPartitionJoinColumns(List<RuntimeFilterEntry> nonPartitionJoinColumns) {
    this.nonPartitionJoinColumns = nonPartitionJoinColumns;
  }

  public List<RuntimeFilterEntry> getPartitionJoinColumns() {
    return partitionJoinColumns;
  }

  public void setPartitionJoinColumns(List<RuntimeFilterEntry> partitionJoinColumns) {
    this.partitionJoinColumns = partitionJoinColumns;
  }

  public boolean isBroadcastJoin() {
    return isBroadcastJoin;
  }

  public void setBroadcastJoin(boolean broadcastJoin) {
    isBroadcastJoin = broadcastJoin;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("");
    if (!nonPartitionJoinColumns.isEmpty()) {
      sb.append(nonPartitionJoinColumns);
    }
    if(!partitionJoinColumns.isEmpty()) {
      sb.append(partitionJoinColumns);
    }
    return sb.toString();
  }
}
