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
package com.dremio.exec.store.dfs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.exec.util.VectorUtil;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Table function converts input data file path and file size, and generates a VarBinary, which
 * contains serialised SplitAndPartitionInfo
 */
public class EasySplitGenTableFunction extends SplitGenTableFunction {
  private static final Logger logger = LoggerFactory.getLogger(EasySplitGenTableFunction.class);

  public EasySplitGenTableFunction(FragmentExecutionContext fec, OperatorContext context, TableFunctionConfig functionConfig) {
    super(fec, context, functionConfig);
  }

  @Override
  public VectorAccessible setup(VectorAccessible accessible) throws Exception {
    super.setup(accessible);
    return outgoing;
  }

  @Override
  protected void initializeIncomingVectors() {
    pathVector = (VarCharVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    sizeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_SIZE);
    modTimeVector = (BigIntVector) VectorUtil.getVectorFromSchemaPath(incoming, MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.MODIFICATION_TIME);
  }

  @Override
  protected void setCurrentPath(int row) {
    currentPath = new String(pathVector.get(row), StandardCharsets.UTF_8);
  }

  @Override
  protected List<SplitAndPartitionInfo> createSplits(String path, long mtime, int maxRecords, List<SplitIdentity> splitsIdentity) {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo
            .newBuilder();
    partitionInfoBuilder.setId(String.valueOf(1));

    PartitionProtobuf.NormalizedPartitionInfo partitionInfo = partitionInfoBuilder.build();
    final List<SplitAndPartitionInfo> splits = new ArrayList<>();
      final EasyProtobuf.EasyDatasetSplitXAttr splitExtended =
        EasyProtobuf.EasyDatasetSplitXAttr.newBuilder()
                      .setStart(this.currentStart)
                      .setPath(path)
                      .setLength(fileSize)
                      .build();

      splitsIdentity.add(new SplitIdentity(splitExtended.getPath(), splitExtended.getStart(), splitExtended.getLength(), fileSize));

      final PartitionProtobuf.NormalizedDatasetSplitInfo.Builder splitInfo = PartitionProtobuf.NormalizedDatasetSplitInfo
              .newBuilder()
              .setPartitionId(partitionInfo.getId())
              .setExtendedProperty(splitExtended.toByteString());
      splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo.build()));
      remainingSize = 0;
    return splits;
  }
}
