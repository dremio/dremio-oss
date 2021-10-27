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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.arrow.util.Preconditions;

import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.metadatarefresh.committer.AbstractReadSignatureProvider;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.hive.proto.HiveReaderProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * {@link ReadSignatureProvider} to be used during full metadata refresh of Hive tables
 */
public class HiveFullRefreshReadSignatureProvider extends AbstractReadSignatureProvider {

  protected static final String EMPTY_STRING = "";
  protected final List<String> partitionPaths;
  protected final Set<String> pathsInReadSignature;

  public HiveFullRefreshReadSignatureProvider(String tableRoot, long queryStartTime, List<String> partitionPaths, Predicate<String> partitionExists) {
    super(tableRoot, queryStartTime, partitionExists);
    this.partitionPaths = partitionPaths;
    pathsInReadSignature = new HashSet<>();
  }

  public ByteString compute(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
    // Add table root dir for non-partitioned table and if not already present
    if (partitionPaths.size() == 0 && (pathsInReadSignature.size() == 0 || !pathsInReadSignature.contains(tableRoot))) {
      pathsInReadSignature.add(tableRoot);
    }
    else {
      assertPartitionsCount(addedPartitions, deletedPartitions);
      pathsInReadSignature.addAll(partitionPaths);
    }
    Preconditions.checkState(pathsInReadSignature.size() != 0, "No paths added to compute read signature");
    final List<HiveReaderProto.FileSystemPartitionUpdateKey> fileSystemPartitionUpdateKeys = new ArrayList<>();

    pathsInReadSignature
      .stream()
      .filter(path -> doesPartitionExist.test(path))
      .forEach(path -> fileSystemPartitionUpdateKeys.add(createFileSystemPartitionUpdateKey(path, queryStartTime)));

    return fileSystemPartitionUpdateKeys.isEmpty() ? ByteString.EMPTY : HiveReaderProto.HiveReadSignature.newBuilder()
      .setType(HiveReaderProto.HiveReadSignatureType.FILESYSTEM)
      .addAllFsPartitionUpdateKeys(fileSystemPartitionUpdateKeys)
      .build()
      .toByteString();
  }

  protected void assertPartitionsCount(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
    Preconditions.checkArgument(addedPartitions.size() <= partitionPaths.size());
    Preconditions.checkArgument(deletedPartitions.size() == 0, "Delete data file operation not allowed in full metadata refresh");
  }

  static HiveReaderProto.HiveReadSignature decodeHiveReadSignatureByteString(ByteString readSig) {
    try {
      return HiveReaderProto.HiveReadSignature.parseFrom(readSig);
    } catch (InvalidProtocolBufferException e) {
      // Wrap protobuf exception for consistency
      throw new RuntimeException(e);
    }
  }

  static HiveReaderProto.FileSystemPartitionUpdateKey createFileSystemPartitionUpdateKey(String partitionPath, long mtime) {
    final List<HiveReaderProto.FileSystemCachedEntity> cachedEntities = new ArrayList<>();
    cachedEntities.add(HiveReaderProto.FileSystemCachedEntity.newBuilder()
      .setPath(EMPTY_STRING)
      .setLastModificationTime(mtime)
      .setIsDir(true)
      .build());

    return HiveReaderProto.FileSystemPartitionUpdateKey.newBuilder()
      .setPartitionId(0) // field unused in code so setting random value
      .setPartitionRootDir(partitionPath)
      .addAllCachedEntities(cachedEntities)
      .build();
  }
}
