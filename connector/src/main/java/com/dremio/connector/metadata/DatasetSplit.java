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
package com.dremio.connector.metadata;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Interface for a connector to provide Affinities related to a split, as well as store connector
 * specific properties to be used during execution.
 */
public interface DatasetSplit {

  /**
   * Get a list of Affinity objects.
   *
   * @return list of affinity objects, not null
   */
  default List<DatasetSplitAffinity> getAffinities() {
    return Collections.emptyList();
  }

  /**
   * Get the size of the split in bytes.
   *
   * @return size of split in bytes
   */
  long getSizeInBytes();

  /**
   * Get the number of records in the split.
   *
   * @return number of records
   */
  long getRecordCount();

  /**
   * Get any additional information about the split.
   *
   * <p>This will be provided by the catalog to other modules that request the catalog about the
   * split, so any custom state could be returned.
   *
   * @return extra information, not null
   */
  default BytesOutput getExtraInfo() {
    return BytesOutput.NONE;
  }

  /**
   * Create {@code DatasetSplit}.
   *
   * @param sizeInBytes size in bytes
   * @param recordCount record count
   * @return dataset split
   */
  static DatasetSplit of(long sizeInBytes, long recordCount) {
    return of(Collections.emptyList(), sizeInBytes, recordCount);
  }

  /**
   * Create {@code DatasetSplit}.
   *
   * @param affinities list of affinities
   * @param sizeInBytes size in bytes
   * @param recordCount record count
   * @return dataset split
   */
  static DatasetSplit of(
      List<DatasetSplitAffinity> affinities, long sizeInBytes, long recordCount) {
    return of(affinities, sizeInBytes, recordCount, BytesOutput.NONE);
  }

  /**
   * Create {@code DatasetSplit}.
   *
   * @param affinities list of affinities
   * @param sizeInBytes size in bytes
   * @param recordCount record count
   * @param extraInfo extra info
   * @return dataset split
   */
  static DatasetSplit of(
      List<DatasetSplitAffinity> affinities,
      long sizeInBytes,
      long recordCount,
      BytesOutput extraInfo) {
    Objects.requireNonNull(affinities, "affinities is required");
    Objects.requireNonNull(extraInfo, "extra info is required");

    return new DatasetSplitImpl(affinities, sizeInBytes, recordCount, extraInfo);
  }
}
