/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.physical.base;

import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Marks classes that should be considered for memory calculation considerations.
 */
public interface MemoryCalcConsidered extends PhysicalOperator {

  /**
   * Whether or not this operation should have a limited memory consumption envelope. Sometimes an
   * operation should be considered for calculation purposes but not actually bounded since the
   * moment it hit a bound, it would die anyway. As memory-bounding is allowed in more operations,
   * they can override this method to return true.
   *
   * @return true if operation should be limited.
   */
  default boolean shouldBeMemoryBounded(OptionManager options) {
    return false;
  }

  /**
   * Set the max allocation of this operation independent of other available memory to the fragment.
   *
   * @param value The amount of memory in bytes.
   */
  void setMaxAllocation(long value);


  /**
   * How much to weight this particular operator within memory calculations.
   * @return 1.0 for average weight, >1 should get more than average, <1 to get less than average.
   */
  @JsonIgnore
  default double getMemoryFactor(OptionManager options) {
    return 1.0d;
  }
}
