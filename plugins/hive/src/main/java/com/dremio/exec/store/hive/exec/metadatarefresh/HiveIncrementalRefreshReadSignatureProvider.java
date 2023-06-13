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

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.metadatarefresh.committer.ReadSignatureProvider;
import com.dremio.hive.proto.HiveReaderProto;
import com.google.protobuf.ByteString;

/**
 * {@link ReadSignatureProvider} to be used during incremental metadata refresh of Hive tables
 * Updates read signature of all the existing and new path discovered
 */
public class HiveIncrementalRefreshReadSignatureProvider extends HiveFullRefreshReadSignatureProvider {

  private final HiveReaderProto.HiveReadSignature existingReadSignature;

  public HiveIncrementalRefreshReadSignatureProvider(ByteString existingReadSignature,
                                                     final String dataTableRoot,
                                                     final long queryStartTime,
                                                     List<String> partitionPaths,
                                                     Predicate<String> partitionExists) {
    super(dataTableRoot, queryStartTime, partitionPaths, partitionExists);
    this.existingReadSignature = decodeHiveReadSignatureByteString(existingReadSignature);
  }

  @Override
  protected void assertPartitionsCount(Set<IcebergPartitionData> addedPartitions, Set<IcebergPartitionData> deletedPartitions) {
  }
}
