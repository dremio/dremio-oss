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
package com.dremio.plugins.sysflight;

import java.util.Iterator;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.record.BatchSchema;

/**
 * Wrapper class for around System table handlers.
 *
 * Use {@link SystemTableWrapper#wrap(T)} to create instances
 *
 * @param <T> a system table handler
 */
final class SystemTableWrapper<T extends DatasetHandle & DatasetMetadata & PartitionChunkListing>
  implements DatasetHandle, DatasetMetadata, PartitionChunkListing {

  private final T systemTable;

  private SystemTableWrapper(T systemTable) {
    this.systemTable = systemTable;
  }

  @Override
  public EntityPath getDatasetPath() {
    return systemTable.getDatasetPath();
  }

  @Override
  public DatasetStats getDatasetStats() {
    return systemTable.getDatasetStats();
  }

  @Override
  public BatchSchema getRecordSchema() {
    return (BatchSchema) systemTable.getRecordSchema();
  }

  @Override
  public Iterator<? extends PartitionChunk> iterator() {
    return systemTable.iterator();
  }

  /**
   * Wrap a systemTable into a {@code SystemTableWrapper} instance
   *
   * @param systemTable the systemTable
   * @return the wrapped instance, or directly the systemTable if already wrapped
   */
  static <T extends DatasetHandle & DatasetMetadata & PartitionChunkListing> SystemTableWrapper wrap(final T systemTable) {
    // No need to wrap if already implementing SystemTableWrapper
    if (systemTable instanceof SystemTableWrapper) {
      return (SystemTableWrapper) systemTable;
    }
    // Create a wrapper
    return new SystemTableWrapper(systemTable);
  }
}

