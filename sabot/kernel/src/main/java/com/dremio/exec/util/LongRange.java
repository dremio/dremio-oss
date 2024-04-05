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
package com.dremio.exec.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/** Basic serializable implementation of Long range. */
public class LongRange {
  private long minInclusive;
  private long maxInclusive;

  @JsonCreator
  public LongRange(@JsonProperty("min") long minInclusive, @JsonProperty("max") long maxInclusive) {
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;
  }

  @JsonProperty("min")
  public long getMinInclusive() {
    return minInclusive;
  }

  @JsonProperty("max")
  public long getMaxInclusive() {
    return maxInclusive;
  }

  public boolean isNotInRange(long value) {
    return value < minInclusive || value > maxInclusive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LongRange longRange = (LongRange) o;
    return minInclusive == longRange.minInclusive && maxInclusive == longRange.maxInclusive;
  }

  @Override
  public int hashCode() {
    return Objects.hash(minInclusive, maxInclusive);
  }

  @Override
  public String toString() {
    return String.format("[%d, %d]", minInclusive, maxInclusive);
  }
}
