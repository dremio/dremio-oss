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
package com.dremio.connector.metadata.options;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import java.util.List;
import java.util.Map;

/** Specifies the type of filter for a metadata refresh and the list of files/partitions. */
public class RefreshTableFilterOption implements GetDatasetOption, ListPartitionChunkOption {
  private final List<String> filesList;
  private final Map<String, String> partition;

  /**
   * Creates a new RefreshTableFilterOption with the given filename list.
   *
   * @param filesList list of filenames that the new object will contain
   */
  public RefreshTableFilterOption(List<String> filesList) {
    this.filesList = filesList;
    this.partition = null;
  }

  /**
   * Creates a new RefreshTableFilterOption with the given partition.
   *
   * @param partition map of key-value pairs to specify a partition
   */
  public RefreshTableFilterOption(Map<String, String> partition) {
    this.filesList = null;
    this.partition = partition;
  }

  /**
   * Is a files list available.
   *
   * @return whether a files list is available
   */
  public boolean isFilesList() {
    return filesList != null;
  }

  /**
   * Is a partition available.
   *
   * @return whether a partition is available
   */
  public boolean isPartition() {
    return partition != null;
  }

  /**
   * Returns the list of filenames.
   *
   * @return the list of filenames
   */
  public List<String> getFilesList() {
    return filesList;
  }

  /**
   * Returns the key-value pairs for a partition. The map maintains the order of the key-value
   * pairs.
   *
   * @return the key-value paris for a partition
   */
  public Map<String, String> getPartition() {
    return partition;
  }
}
