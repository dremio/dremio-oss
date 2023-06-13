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
package com.dremio.exec.planner.common;

import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Utility class to provide helper methods to read the partition stats file
 */
public class PartitionStatsHelper {
  public static ImmutableDremioFileAttrs getPartitionStatsFileAttrs(ScanRelBase drel) {
    String fileName;
    Long fileLength;

    if(DatasetHelper.isInternalIcebergTable(drel.getTableMetadata().getDatasetConfig())) {
      IcebergMetadata icebergMetadata = drel.getTableMetadata().getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
      fileName = icebergMetadata.getPartitionStatsFile();
      fileLength = icebergMetadata.getPartitionStatsFileSize();
    } else {
      byte[] byteBuffer = drel.getTableMetadata().getReadDefinition().getExtendedProperty().toByteArray();

      IcebergProtobuf.IcebergDatasetXAttr icebergDatasetXAttr;
      try {
        icebergDatasetXAttr = LegacyProtobufSerializer.parseFrom(IcebergProtobuf.IcebergDatasetXAttr.PARSER, byteBuffer);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
      fileName = icebergDatasetXAttr.getPartitionStatsFile();
      fileLength = (icebergDatasetXAttr.hasPartitionStatsFileSize()) ? icebergDatasetXAttr.getPartitionStatsFileSize() : null;
    }

    return new ImmutableDremioFileAttrs.Builder()
      .setFileName(fileName)
      .setFileLength(fileLength)
      .build();
  }
}
