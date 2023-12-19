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
package com.dremio.exec.planner.sql.parser;

/**
 * Distribution strategy for data within a partition.
 */
public enum PartitionDistributionStrategy {

  /**
   * Policy is not set. System makes a choice.
   */
  UNSPECIFIED,

  /**
   * Consolidate data within a partition on one node (which maybe in multiple files, but few files).
   * <p>
   * This will very likely result in large data shuffle across nodes to consolidate data for each partition. Execution
   * will very likely be slower.
   */
  HASH,

  /**
   * TODO: ...
   */
  ROUND_ROBIN,

  /**
   * Stripe data within a partition across nodes. Each file still contains data within a partition.
   * <p>
   * There is no data transfer to consolidate each partition. Execution will very likely be faster.
   */
  STRIPED;

  public static PartitionDistributionStrategy getPartitionDistributionStrategy(String optionName) {
    try {
      return PartitionDistributionStrategy.valueOf(optionName);
    } catch (Exception e) {
      // The optionName may be the lower case string (e.g. java)
      for(PartitionDistributionStrategy option : PartitionDistributionStrategy.values()) {
        if (optionName.equalsIgnoreCase(option.toString())) {
          return option;
        }
      }
    }

    // unreachable code. The validator ensures that the value is one of the available choices
    return UNSPECIFIED;
  }
}
