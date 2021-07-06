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
package com.dremio.exec.store.hive.exec;

import static com.dremio.exec.store.hive.metadata.HiveMetadataUtils.buildHiveSplitXAttr;
import static org.apache.hadoop.hive.ql.io.IOConstants.AVRO;
import static org.apache.hadoop.hive.ql.io.IOConstants.ORC;
import static org.apache.hadoop.hive.ql.io.IOConstants.PARQUET;

import java.util.Collections;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.hive.metadata.ParquetInputFormat;
import com.dremio.hive.proto.HiveReaderProto;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Creates hive input split and Partition Info for different file formats
 */
public class HiveSplitCreator implements BlockBasedSplitGenerator.SplitCreator {

  public SplitAndPartitionInfo createSplit(PartitionProtobuf.NormalizedPartitionInfo filePartitionInfo, SplitIdentity splitIdentity, String fileFormat, long fileSize, long currentModTime) {
    InputSplit inputSplit;
    switch (fileFormat.toUpperCase()) {
      case PARQUET:
        inputSplit = new ParquetInputFormat.ParquetSplit(new Path(splitIdentity.getPath()), splitIdentity.getOffset(), splitIdentity.getLength(), null, fileSize, currentModTime);
        break;
      case ORC:
        inputSplit = new OrcSplit(new Path(splitIdentity.getPath()), null, splitIdentity.getOffset(),
          splitIdentity.getLength(), null, null, true, false,
          Collections.emptyList(), -1, fileSize);
        break;
      case AVRO:
        inputSplit = new FileSplit(new Path(splitIdentity.getPath()), splitIdentity.getOffset(), splitIdentity.getLength(), (String[])null);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file format type " + fileFormat);
    }

    HiveReaderProto.HiveSplitXattr splitExtended = buildHiveSplitXAttr(Integer.valueOf(filePartitionInfo.getId()), inputSplit);

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
      .setPartitionId(filePartitionInfo.getId())
      .setExtendedProperty(splitExtended.toByteString());

    return new SplitAndPartitionInfo(filePartitionInfo, splitInfo.build());
  }
}
