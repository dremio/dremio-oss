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

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import com.dremio.exec.store.file.proto.FileProtobuf;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.google.protobuf.ByteString;

/**
 * {@link ReadSignatureProvider} to be used during full metadata refresh
 */
public class FullRefreshReadSignatureProvider extends AbstractReadSignatureProvider {

  protected final Set<String> pathsInReadSignature;
  private final Function<String, FileProtobuf.FileSystemCachedEntity> FCEfromPathMtime =
    path -> FileProtobuf.FileSystemCachedEntity.newBuilder()
      .setPath(path)
      .setLastModificationTime(queryStartTime)
      .build();

  public FullRefreshReadSignatureProvider(String tableRoot, long queryStartTime) {
    this(tableRoot, queryStartTime, path -> true);
  }

  protected FullRefreshReadSignatureProvider(String tableRoot, long queryStartTime,
                                             Predicate<String> partitionExists) {
    super(tableRoot, queryStartTime, partitionExists);
    pathsInReadSignature = new HashSet<>();
  }


  /**
   * Creates read signature with all the added partitions.
   * Throws {@link IllegalStateException} if deletedPartitions is not empty
   * @param addedPartitions
   * @param deletedPartitions
   * @return
   */
  @Override
  public ByteString compute(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {

    handleAddedPartitions(addedPartitions);
    handleDeletedPartitions(deletedPartitions);

    FileProtobuf.FileUpdateKey.Builder readSigBuilder = FileProtobuf.FileUpdateKey.newBuilder();
    // add root
    pathsInReadSignature.remove(tableRoot);
    readSigBuilder.addCachedEntities(FCEfromPathMtime.apply(tableRoot)); // first entry should be the root

    pathsInReadSignature.stream()
      .map(FCEfromPathMtime)
      .forEach(readSigBuilder::addCachedEntities);
    return readSigBuilder.build().toByteString();
  }

  protected void handleAddedPartitions(Set<IcebergPartitionData> added) {
    added.stream()
      .map(fileSystemPartitionToPathMapper)
      .flatMap(Collection::stream).filter(Objects::nonNull)
      .forEach(pathsInReadSignature::add);
  }

  protected void handleDeletedPartitions(Set<IcebergPartitionData> deleted) {
    if (deleted.size() > 0) {
      throw new IllegalStateException("Delete data file operation not allowed in full metadata refresh");
    }
  }
}
