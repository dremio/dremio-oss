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
package com.dremio.exec.store.metadatarefresh.committer;

import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Similar to {@link IncrementalRefreshReadSignatureProvider}, except updates mtime of only modified
 * partitions
 */
public class PartialRefreshReadSignatureProvider extends AbstractReadSignatureProvider {

  private final ByteString oldReadSignatureBytes;
  private final Set<String> partitionsToModify = new HashSet<>();
  private final Set<String> partitionsToDelete = new HashSet<>();

  public PartialRefreshReadSignatureProvider(
      ByteString existingReadSignature,
      final String dataTableRoot,
      final long queryStartTime,
      Predicate<String> partitionExists) {
    super(dataTableRoot, queryStartTime, partitionExists);
    oldReadSignatureBytes = existingReadSignature;
  }

  private final Function<String, FileProtobuf.FileSystemCachedEntity> FCEfromPathMtime =
      path ->
          FileProtobuf.FileSystemCachedEntity.newBuilder()
              .setPath(path)
              .setLastModificationTime(queryStartTime)
              .build();

  @Override
  public ByteString compute(
      Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
    final FileProtobuf.FileUpdateKey oldReadSignature =
        decodeReadSignatureByteString(oldReadSignatureBytes);
    handleAddedPartitions(addedPartitions);
    handleDeletedPartitions(deletedPartitions);

    FileProtobuf.FileUpdateKey.Builder readSigBuilder = FileProtobuf.FileUpdateKey.newBuilder();
    oldReadSignature.getCachedEntitiesList().stream()
        .filter(fce -> !partitionsToDelete.contains(fce.getPath())) // exclude deleted partitions
        .forEach(
            fce -> {
              if (partitionsToModify.contains(
                  fce.getPath())) { // update mtime if partition got modified
                readSigBuilder.addCachedEntities(
                    FileProtobuf.FileSystemCachedEntity.newBuilder()
                        .setPath(fce.getPath())
                        .setLastModificationTime(queryStartTime)
                        .build());
                partitionsToModify.remove(fce.getPath());
              } else { // add without changing mtime if partition was not modified
                readSigBuilder.addCachedEntities(fce);
              }
            });
    // add new partitions
    partitionsToModify.stream().map(FCEfromPathMtime).forEach(readSigBuilder::addCachedEntities);
    return readSigBuilder.build().toByteString();
  }

  private void handleAddedPartitions(Set<IcebergPartitionData> added) {
    added.stream()
        .map(fileSystemPartitionToPathMapper)
        .flatMap(Collection::stream)
        .forEach(partitionsToModify::add);
  }

  private void handleDeletedPartitions(Set<IcebergPartitionData> deleted) {
    deleted.stream()
        .map(fileSystemPartitionToPathMapper)
        .flatMap(x -> x.stream())
        .forEach(partitionsToModify::add);
    partitionsToModify.stream()
        .filter(doesPartitionExist.negate())
        .forEach(partitionsToDelete::add);
    partitionsToModify.removeAll(partitionsToDelete);
  }
}
