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
package com.dremio.service.namespace;

import java.io.IOException;

import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * The DatasetMetadataSaver is used to save the metadata of a single dataset
 * The methods need to be invoked in order. In particular, the partition chunk listing must be saved then the dataset.
 * Saving of the dataset always completes with the saving of the dataset config
 * Order of invocation:
 *    - partition chunk listing
 *  - dataset config         // "closes" the save. Refers to all the partition chunks saved above
 */
public interface DatasetMetadataSaver extends AutoCloseable {

  /**
   * Save a partition chunk listing that refers to all partition chunks since the last invocation
   * of {@link #savePartitionChunks(PartitionChunkListing)}, or since the creation of this metadata saver, whichever came last.
   * Also calculates to total number of records across every split and every partition chunk listed.
   *
   * @param chunkListing The partition chunks to save.
   * @return The total record count of all splits in chunkListing.
   */
  long savePartitionChunks(PartitionChunkListing chunkListing) throws IOException;

  /**
   * Complete the saving of this dataset. After this method is invoked, invoking any other method on this saver will
   * throw a RuntimeException
   * It's only legal to call this method after a partition chunk has been saved. Invoking this method without saving
   * a partition chunk will throw a runtime exception, as will invoking this method directly after saving a dataset split
   */
  void saveDataset(DatasetConfig datasetConfig, boolean opportunisticSave, NamespaceAttribute... attributes) throws NamespaceException;

  /**
   * If {@link #saveDataset} was not invoked, will clean up any effects of
   * {@link #savePartitionChunks(PartitionChunkListing)} calls.
   */
  @Override
  void close();
}
