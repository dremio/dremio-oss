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

import java.io.IOException;
import java.util.Iterator;

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.service.namespace.DatasetMetadataSaver;

public final class CatalogUtil {
  private CatalogUtil() {
  }

  /**
   * Save a partition chunk listing that refers to all partition chunks since the last invocation
   * of {@link #savePartitionChunksInSplitsStores(DatasetMetadataSaver, PartitionChunkListing)}, or since the creation of this metadata saver, whichever came last.
   * Also calculates to total number of records across every split and every partition chunk listed.
   *
   * @param chunkListing The partition chunks to save.
   * @return The total record count of all splits in chunkListing.
   */
  public static long savePartitionChunksInSplitsStores(DatasetMetadataSaver saver, PartitionChunkListing chunkListing) throws IOException {
    long recordCountFromSplits = 0;
    final Iterator<? extends PartitionChunk> chunks = chunkListing.iterator();
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();

      final Iterator<? extends DatasetSplit> splits = chunk.getSplits().iterator();
      while (splits.hasNext()) {
        final DatasetSplit split = splits.next();
        saver.saveDatasetSplit(split);
        recordCountFromSplits += split.getRecordCount();
      }
      saver.savePartitionChunk(chunk);
    }
    return recordCountFromSplits;
  }
}
