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
package com.dremio.exec.store.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetNotFoundException;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.catalog.DatasetTypeHandle;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

@Options
public interface FileDatasetHandle extends DatasetTypeHandle {
  TypeValidators.LongValidator DFS_MAX_FILES = new TypeValidators.RangeLongValidator("dremio.store.dfs.max_files", 1L, Integer.MAX_VALUE, 20000000L);
  final Logger logger = LoggerFactory.getLogger(FileDatasetHandle.class);

  DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException;

  PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException;

  BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException;

  /**
   * Throw an exception if the given dataset has too many files in it. Only applies to external storage plugins.
   *
   * @param datasetName         The name of the dataset being tested for too many files.
   * @param numFilesInDirectory The number of actual files in the dataset.
   * @param context             The context the check is being run within.
   * @param isInternal          Flag to indicate if the check is being done with an internal source such as the AccelerationStoragePlugin.
   */
  static void checkMaxFiles(String datasetName, int numFilesInDirectory, SabotContext context, boolean isInternal) throws FileCountTooLargeException {
    if (!isInternal) {
      final int maxFiles = getMaxFilesLimit(context);
      if (numFilesInDirectory > maxFiles) {
        throw new FileCountTooLargeException(datasetName, numFilesInDirectory, maxFiles);
      }
    }
  }

  static int getMaxFilesLimit(SabotContext context) {
    return Math.toIntExact(context.getOptionManager().getOption(FileDatasetHandle.DFS_MAX_FILES));
  }

  default boolean metadataValid(BytesOutput readSignature, DatasetHandle datasetHandle, DatasetMetadata metadata, FileSystem fileSystem) throws DatasetNotFoundException {
    final FileProtobuf.FileUpdateKey fileUpdateKey;
    try {
      fileUpdateKey = LegacyProtobufSerializer.parseFrom(FileProtobuf.FileUpdateKey.PARSER, MetadataProtoUtils.toProtobuf(readSignature));
    } catch (InvalidProtocolBufferException e) {
      // Wrap protobuf exception for consistency
      throw new RuntimeException(e);
    }

    if (fileUpdateKey.getCachedEntitiesList() == null || fileUpdateKey.getCachedEntitiesList().isEmpty()) {
      // TODO: evaluate the need for this.
//       Preconditions.checkArgument(oldConfig.getType() == DatasetType.PHYSICAL_DATASET_SOURCE_FILE,
//           "only file based datasets can have empty read signature");
      // single file dataset
      return false;
    }
    final UpdateStatus status = checkMultifileStatus(fileUpdateKey, fileSystem);

    switch(status) {
      case DELETED:
        throw new DatasetNotFoundException(datasetHandle.getDatasetPath());
      case UNCHANGED:
        return true;
      case CHANGED:
        return false;
      default:
        throw new UnsupportedOperationException(status.name());
    }
  }

  /**
   * Given a file update key, determine whether the source system has changed since we last read the status.
   *
   * @param fileUpdateKey
   * @return The type of status change.
   */
  static UpdateStatus checkMultifileStatus(FileProtobuf.FileUpdateKey fileUpdateKey, FileSystem fileSystem) {
    final List<FileProtobuf.FileSystemCachedEntity> cachedEntities = fileUpdateKey.getCachedEntitiesList();
    for (int i = 0; i < cachedEntities.size(); ++i) {
      final FileProtobuf.FileSystemCachedEntity cachedEntity = cachedEntities.get(i);
      final Path cachedEntityPath = Path.of(cachedEntity.getPath());
      try {

        try {
          final FileAttributes updatedFileAttributes = fileSystem.getFileAttributes(cachedEntityPath);
          final long updatedModificationTime = updatedFileAttributes.lastModifiedTime().toMillis();
          Preconditions.checkArgument(updatedFileAttributes.isDirectory(), "fs based dataset update key must be composed of directories");
          if (cachedEntity.getLastModificationTime() < updatedModificationTime) {
            // the file/folder has been changed since our last check.
            return UpdateStatus.CHANGED;
          }
        } catch (FileNotFoundException e) {
          // if first entity (root) is missing then table is deleted
          if (i == 0) {
            return UpdateStatus.DELETED;
          }
          // missing directory force update for this dataset
          return UpdateStatus.CHANGED;
        }

        if (cachedEntity.getLastModificationTime() == 0) {
          // this system doesn't support modification times, no need to further probe (S3)
          return UpdateStatus.CHANGED;
        }
      } catch (IOException ioe) {
        // continue with other cached entities
        logger.error("Failed to get status for {}", cachedEntityPath, ioe);
        return UpdateStatus.CHANGED;
      }
    }

    return UpdateStatus.UNCHANGED;
  }


  enum UpdateStatus {
    /**
     * Metadata hasn't changed.
     */
    UNCHANGED,


    /**
     * Metadata has changed.
     */
    CHANGED,

    /**
     * Dataset has been deleted.
     */
    DELETED
  }
}
