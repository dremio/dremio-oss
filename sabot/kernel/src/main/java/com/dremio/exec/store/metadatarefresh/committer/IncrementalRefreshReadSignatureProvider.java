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
import java.util.Set;
import java.util.function.Predicate;

/**
 * Computes read signature in incremental refresh newReadSignature = oldReadSignature +
 * addedPartitions - deletedPartitions Updates mtime of all partitions in read signature
 */
public class IncrementalRefreshReadSignatureProvider extends FullRefreshReadSignatureProvider {

  public IncrementalRefreshReadSignatureProvider(
      ByteString existingReadSignature,
      final String dataTableRoot,
      final long queryStartTime,
      Predicate<String> partitionExists) {
    super(dataTableRoot, queryStartTime, partitionExists);
    FileProtobuf.FileUpdateKey oldReadSignature =
        decodeReadSignatureByteString(existingReadSignature);
    oldReadSignature.getCachedEntitiesList().stream()
        .map(FileProtobuf.FileSystemCachedEntity::getPath)
        .forEach(pathsInReadSignature::add);
  }

  @Override
  protected void handleDeletedPartitions(Set<IcebergPartitionData> deleted) {
    deleted.stream()
        .map(fileSystemPartitionToPathMapper)
        .flatMap(Collection::stream)
        .filter(doesPartitionExist.negate())
        .forEach(pathsInReadSignature::remove);
  }
}
