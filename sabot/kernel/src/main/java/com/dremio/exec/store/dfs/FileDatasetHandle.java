/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.dfs;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.DatasetTypeHandle;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;

@Options
public interface FileDatasetHandle extends DatasetTypeHandle {
  TypeValidators.LongValidator DFS_MAX_FILES = new TypeValidators.RangeLongValidator("dremio.store.dfs.max_files", 1L, Integer.MAX_VALUE, 20000000L);

  DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException;
  PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException;
  BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException;

  /**
   * Throw an exception if the given dataset has too many files in it. Only applies to external storage plugins.
   *
   * @param datasetName
   *        The name of the dataset being tested for too many files.
   * @param numFilesInDirectory
   *        The number of actual files in the dataset.
   * @param context
   *        The context the check is being run within.
   * @param isInternal
   *        Flag to indicate if the check is being done with an internal source such as the AccelerationStoragePlugin.
   */
  static void checkMaxFiles(String datasetName, int numFilesInDirectory, SabotContext context, boolean isInternal) throws FileCountTooLargeException {
    if (!isInternal) {
      final int maxFiles = Math.toIntExact(context.getOptionManager().getOption(FileDatasetHandle.DFS_MAX_FILES));
      if (numFilesInDirectory > maxFiles) {
        throw new FileCountTooLargeException(datasetName, numFilesInDirectory, maxFiles);
      }
    }
  }
}
