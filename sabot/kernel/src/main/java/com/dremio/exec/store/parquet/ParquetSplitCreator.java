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
package com.dremio.exec.store.parquet;

import static org.apache.iceberg.FileFormat.PARQUET;

import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Preconditions;

/**
 * Creates Parquet Split and partition info
 */
public class ParquetSplitCreator implements BlockBasedSplitGenerator.SplitCreator {

  @Override
  public SplitAndPartitionInfo createSplit(PartitionProtobuf.NormalizedPartitionInfo filePartitionInfo, SplitIdentity splitIdentity,
                                           String fileFormat, long fileSize, long currentModTime) {

  Preconditions.checkArgument(fileFormat.equalsIgnoreCase(PARQUET.toString()));
    ParquetProtobuf.ParquetBlockBasedSplitXAttr splitExtended = ParquetProtobuf.ParquetBlockBasedSplitXAttr.newBuilder()
      .setPath(splitIdentity.getPath())
      .setStart(splitIdentity.getOffset())
      .setLength(splitIdentity.getLength())
      .setFileLength(fileSize)
      .setLastModificationTime(currentModTime)
      .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
      .setPartitionId(filePartitionInfo.getId())
      .setExtendedProperty(splitExtended.toByteString());

    return new SplitAndPartitionInfo(filePartitionInfo, splitInfo.build());
  }
}
