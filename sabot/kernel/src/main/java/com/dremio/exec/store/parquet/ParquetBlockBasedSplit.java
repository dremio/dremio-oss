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

import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.google.protobuf.InvalidProtocolBufferException;

public class ParquetBlockBasedSplit implements Comparable<ParquetBlockBasedSplit> {
  private final SplitAndPartitionInfo splitAndPartitionInfo;
  private final ParquetProtobuf.ParquetBlockBasedSplitXAttr parquetBlockBasedSplitXAttr;

  ParquetBlockBasedSplit(SplitAndPartitionInfo splitAndPartitionInfo) {
    this.splitAndPartitionInfo = splitAndPartitionInfo;
    try {
      this.parquetBlockBasedSplitXAttr =
          ParquetProtobuf.ParquetBlockBasedSplitXAttr.parseFrom(
              splitAndPartitionInfo.getDatasetSplitInfo().getExtendedProperty());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(
          "Failed to parse dataset split for "
              + splitAndPartitionInfo.getPartitionInfo().getSplitKey(),
          e);
    }
  }

  public SplitAndPartitionInfo getSplitAndPartitionInfo() {
    return splitAndPartitionInfo;
  }

  public long getStart() {
    return parquetBlockBasedSplitXAttr.getStart();
  }

  public long getLength() {
    return parquetBlockBasedSplitXAttr.getLength();
  }

  public String getPath() {
    return parquetBlockBasedSplitXAttr.getPath();
  }

  public boolean hasFileLength() {
    return parquetBlockBasedSplitXAttr.hasFileLength();
  }

  public long getFileLength() {
    return parquetBlockBasedSplitXAttr.getFileLength();
  }

  public boolean hasLastModificationTime() {
    return parquetBlockBasedSplitXAttr.hasLastModificationTime();
  }

  public long getLastModificationTime() {
    return parquetBlockBasedSplitXAttr.getLastModificationTime();
  }

  @Override
  public int compareTo(ParquetBlockBasedSplit other) {
    final int ret = getPath().compareTo(other.getPath());
    if (ret == 0) {
      return Long.compare(getStart(), other.getStart());
    }
    return ret;
  }
}
