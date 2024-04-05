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
package com.dremio.exec.catalog;

import java.util.Optional;
import org.immutables.value.Value;

/** State of the metadata (schema and other) in the KV store for a table or view. */
@Value.Immutable
public interface DatasetMetadataState {
  /**
   * Whether {@link com.dremio.service.namespace.dataset.proto.DatasetConfig} has metadata for
   * querying it. A table may be just created from a file or folder, without schema parsed from the
   * file(s), this is an incomplete table.
   */
  boolean isComplete();

  /**
   * Whether the dataset has up-to-date metadata as of the last validity check. A table may be
   * complete but with stale metadata. Metadata may be present but not reflect the current state of
   * the table in the underlying source (schema, iceberg root pointer, etc.) if it has changed
   * externally since the last refresh.
   *
   * <p>For datasets that are always considered up-to-date, such as versioned datasets, this will
   * always return false.
   */
  boolean isExpired();

  /**
   * When the last validity check was done on the metadata. Currently, only Iceberg datasets have
   * explicit validity checks. For most others, this will reflect the last refresh time.
   *
   * <p>For datasets that are always considered up-to-date, such as versioned datasets, this will
   * return the time when the metadata was fetched.
   */
  Optional<Long> lastRefreshTimeMillis();

  /** Convenience method to combine the two flags. */
  default boolean isCompleteAndValid() {
    return isComplete() && !isExpired();
  }

  /** The immutables Builder. */
  static ImmutableDatasetMetadataState.Builder builder() {
    return new ImmutableDatasetMetadataState.Builder();
  }
}
