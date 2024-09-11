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
package com.dremio.service.namespace.split;

import com.dremio.datastore.api.FindByCondition;
import com.dremio.datastore.api.FindByRange;
import com.dremio.service.namespace.PartitionChunkId;
import com.dremio.service.namespace.PartitionChunkMetadata;

/** Namespace operations for splits. */
public interface SplitNamespaceService {
  /** Compression of (multi)splits in the K/V store */
  enum SplitCompression {
    UNCOMPRESSED, // splits stored uncompressed
    SNAPPY // splits stored using snappy compression
  }

  //// READ
  Iterable<PartitionChunkMetadata> findSplits(FindByCondition condition);

  Iterable<PartitionChunkMetadata> findSplits(FindByRange<PartitionChunkId> range);

  int getPartitionChunkCount(FindByCondition condition);

  //// DELETE
  /**
   * Delete any orphaned splits from the Namespace.
   *
   * <p>NOTE: this cannot be run in parallel with any other metadata updates as that may cause
   * generation of split orphans while the dataset is initially getting setup.
   *
   * @param policy the expiration policy. Note: choosing an aggresive policy while running other
   *     metadata updates or planning queries may cause generation of split orphans while the
   *     dataset is initially getting setup, or query errors
   * @return The number of splits deleted.
   */
  int deleteSplitOrphans(
      PartitionChunkId.SplitOrphansRetentionPolicy policy,
      boolean datasetMetadataConsistencyValidate);

  void deleteSplits(Iterable<PartitionChunkId> datasetSplits);
}
