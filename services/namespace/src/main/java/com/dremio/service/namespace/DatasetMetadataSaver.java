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

import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * The DatasetMetadataSaver is used to save the metadata of a single dataset
 * The methods need to be invoked in order. In particular, the partition chunk listing must be saved then the dataset.
 * Saving of the dataset always completes with the saving of the dataset config
 * Order of invocation:
 *      - dataset split +    // one or more invocations to {@link #saveDatasetSplit(DatasetSplit)}
 *    - partition chunk      // refers to all the dataset split(s) above
 *      - dataset split +
 *    - partition chunk      // refers to the dataset split(s) saved after the last partition chunk
 *  - dataset config         // "closes" the save. Refers to all the partition chunks saved above
 */
public interface DatasetMetadataSaver extends AutoCloseable {

  /**
   * Save a single split
   */
  void saveDatasetSplit(DatasetSplit split);

  /**
   * Save a partition chunk that refers to all the dataset splits since the last invocation
   * of {@link #savePartitionChunk(PartitionChunk)}, or since the creation of this metadata saver, whichever came last.
   * At least one {@link #saveDatasetSplit(DatasetSplit)} must have been invoked, or this method will throw a
   * RuntimeException
   */
  void savePartitionChunk(PartitionChunk partitionChunk) throws IOException;

  /**
   * Complete the saving of this dataset. After this method is invoked, invoking any other method on this saver will
   * throw a RuntimeException
   * It's only legal to call this method after a partition chunk has been saved. Invoking this method without saving
   * a partition chunk will throw a runtime exception, as will invoking this method directly after saving a dataset split
   */
  void saveDataset(DatasetConfig datasetConfig, boolean opportunisticSave, NamespaceAttribute... attributes) throws NamespaceException;

  /**
   * If {@link #saveDataset} was not invoked, will clean up any effects of the {@link #saveDatasetSplit(DatasetSplit)}
   * and {@link #savePartitionChunk(PartitionChunk)} calls
   */
  @Override
  void close();
}
