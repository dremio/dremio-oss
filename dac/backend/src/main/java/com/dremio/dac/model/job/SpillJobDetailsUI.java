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
package com.dremio.dac.model.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holder for info from{@link com.dremio.service.job.proto.SpillJobDetails}
 * for representing in UI
 */
public class SpillJobDetailsUI {

  private final boolean hashAggSpilled;
  private final boolean sortSpilled;
  private final long totalBytesSpilled;

  @JsonCreator
  public SpillJobDetailsUI(
    @JsonProperty("hashAggSpilled") boolean hashAggSpilled,
    @JsonProperty("sortSpilled") boolean sortSpilled,
    @JsonProperty("totalBytesSpilled") long totalBytesSpilled) {
    this.hashAggSpilled = hashAggSpilled;
    this.sortSpilled = sortSpilled;
    this.totalBytesSpilled = totalBytesSpilled;
  }

  /**
   * Total data (in bytes) spilled by the query. The total is
   * across all the operators that currently have the spilling ability
   * (external sort and vectorized hashagg as of now)
   * @return total size (in bytes) of spilled data
   */
  public long getTotalBytesSpilled() {
    return totalBytesSpilled;
  }

  public boolean isHashAggSpilled() {
    return hashAggSpilled;
  }

  public boolean isSortSpilled() {
    return sortSpilled;
  }
}
