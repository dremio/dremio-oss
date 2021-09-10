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
package com.dremio.exec.store.hive.exec.metadatarefresh;

import static com.dremio.exec.store.hive.exec.metadatarefresh.HiveFullRefreshReadSignatureProvider.createFileSystemPartitionUpdateKey;
import static com.dremio.exec.store.hive.exec.metadatarefresh.HiveFullRefreshReadSignatureProvider.decodeHiveReadSignatureByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.metadatarefresh.committer.AbstractReadSignatureProvider;
import com.dremio.hive.proto.HiveReaderProto;
import com.google.protobuf.ByteString;

/**
 * Similar to {@link HiveIncrementalRefreshReadSignatureProvider}, except updates mtime of only modified partitions
 */
public class HivePartialRefreshReadSignatureProvider extends AbstractReadSignatureProvider {

  private final ByteString oldReadSignatureBytes;
  private final List<String> partitionPaths;

  public HivePartialRefreshReadSignatureProvider(ByteString existingReadSignature,
                                                 final String dataTableRoot, final long queryStartTime,
                                                 List<String> partitionPaths) {
    super(dataTableRoot, queryStartTime, path -> true);
    oldReadSignatureBytes = existingReadSignature;
    this.partitionPaths = partitionPaths;
  }

  @Override
  public ByteString compute(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
    assertPartitionsCount(addedPartitions, deletedPartitions);
    final HiveReaderProto.HiveReadSignature oldReadSignature = decodeHiveReadSignatureByteString(oldReadSignatureBytes);
    final List<HiveReaderProto.FileSystemPartitionUpdateKey> fileSystemPartitionUpdateKeys = new ArrayList<>();

    oldReadSignature.getFsPartitionUpdateKeysList()
      .stream()
      .forEach(updateKey -> {
        if (partitionPaths.contains(updateKey.getPartitionRootDir())) { // update mtime if partition got modified
          fileSystemPartitionUpdateKeys.add(createFileSystemPartitionUpdateKey(updateKey.getPartitionRootDir(), queryStartTime));
          partitionPaths.remove(updateKey.getPartitionRootDir());
        } else { // add without changing mtime if partition was not modified
          fileSystemPartitionUpdateKeys.add(updateKey);
        }
      });
    // add new partitions
    partitionPaths.stream()
      .forEach(path -> fileSystemPartitionUpdateKeys.add(createFileSystemPartitionUpdateKey(path, queryStartTime)));

    return HiveReaderProto.HiveReadSignature.newBuilder()
      .setType(HiveReaderProto.HiveReadSignatureType.FILESYSTEM)
      .addAllFsPartitionUpdateKeys(fileSystemPartitionUpdateKeys)
      .build()
      .toByteString();
  }

  private void assertPartitionsCount(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
    Preconditions.checkArgument(deletedPartitions.size() == 0, "Delete data file operation not allowed in Hive partial metadata refresh");
  }
}
