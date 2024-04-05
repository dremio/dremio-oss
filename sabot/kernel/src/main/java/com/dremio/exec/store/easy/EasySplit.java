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
package com.dremio.exec.store.easy;

import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.google.protobuf.InvalidProtocolBufferException;

public class EasySplit implements Comparable<EasySplit> {
  private final SplitAndPartitionInfo splitAndPartitionInfo;
  private final EasyProtobuf.EasyDatasetSplitXAttr easySplitXAttr;

  EasySplit(SplitAndPartitionInfo splitAndPartitionInfo) {
    this.splitAndPartitionInfo = splitAndPartitionInfo;
    try {
      this.easySplitXAttr =
          EasyProtobuf.EasyDatasetSplitXAttr.parseFrom(
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
    return easySplitXAttr.getStart();
  }

  public long getLength() {
    return easySplitXAttr.getLength();
  }

  public String getPath() {
    return easySplitXAttr.getPath();
  }

  public boolean hasFileLength() {
    return easySplitXAttr.hasLength();
  }

  public long getFileLength() {
    return easySplitXAttr.getLength();
  }

  public boolean hasLastModificationTime() {
    return false;
  }

  public long getLastModificationTime() {
    return 0;
  }

  @Override
  public int compareTo(EasySplit other) {
    final int ret = getPath().compareTo(other.getPath());
    if (ret == 0) {
      return Long.compare(getStart(), other.getStart());
    }
    return ret;
  }
}
