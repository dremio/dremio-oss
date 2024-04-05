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
package com.dremio.datastore.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * Configuration for finding values by a key range. Start range and end range can be {@code null} to
 * specify an unbounded range search.
 *
 * @param <K> the key type
 */
@JsonDeserialize(builder = ImmutableFindByRange.Builder.class)
@Immutable
public interface FindByRange<K> {
  /**
   * Get the starting range.
   *
   * @return starting range of type K. Returns {@code null} if start range is null.
   */
  @Nullable
  @Value.Default
  default K getStart() {
    return null;
  }

  /**
   * Get the ending range.
   *
   * @return ending range of type K. Returns {@code null} if end range is null.
   */
  @Nullable
  @Value.Default
  default K getEnd() {
    return null;
  }

  /**
   * Indicates whether the starting range is inclusive.
   *
   * @return true if starting range is inclusive, false otherwise.
   */
  @Value.Default
  default boolean isStartInclusive() {
    return false;
  }

  /**
   * Indicates whether the ending range is inclusive.
   *
   * @return true if ending range is inclusive, false otherwise.
   */
  @Value.Default
  default boolean isEndInclusive() {
    return false;
  }
}
