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

import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/** Creates Easy Split and partition info */
public class EasySplitCreator implements BlockBasedSplitGenerator.SplitCreator {
  private final OperatorContext context;

  public EasySplitCreator(OperatorContext context, boolean convertToRelativePath) {
    this.context = context;
  }

  @Override
  public SplitAndPartitionInfo createSplit(
      PartitionProtobuf.NormalizedPartitionInfo filePartitionInfo,
      SplitIdentity splitIdentity,
      String fileFormat,
      long fileSize,
      long currentModTime) {

    EasyProtobuf.EasyDatasetSplitXAttr splitExtended =
        EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
            .setPath(splitIdentity.getPath())
            .setStart(splitIdentity.getOffset())
            .setLength(splitIdentity.getLength())
            .setLength(fileSize)
            .build();

    PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo =
        PartitionProtobuf.NormalizedDatasetSplitInfo.newBuilder()
            .setPartitionId(filePartitionInfo.getId())
            .setExtendedProperty(splitExtended.toByteString());

    return new SplitAndPartitionInfo(filePartitionInfo, splitInfo.build());
  }

  @Override
  public long getTargetSplitSize(String fileFormat) {
    return 1;
  }
}
