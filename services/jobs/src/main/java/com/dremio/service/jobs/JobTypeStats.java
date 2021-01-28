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
package com.dremio.service.jobs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a certain job type stats
 */
public class JobTypeStats {
  /**
   * Enum of all possible Job Types we collect stats for.
   */
  public enum Types { UI, EXTERNAL, ACCELERATION, DOWNLOAD, INTERNAL };

  private final Types type;
  private final int count;

  @JsonCreator
  public JobTypeStats(
      @JsonProperty("type") Types type,
      @JsonProperty("count") int count) {
    this.type = type;
    this.count = count;
  }

  public Types getType() {
    return type;
  }

  public int getCount() {
    return count;
  }
}
