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
package com.dremio.exec.store.deltalake;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.easy.EasyGroupScan;
import com.dremio.exec.store.dfs.easy.EasySubScan;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;

/**
 * DeltaLake dataset's group scan. The expected output schema to include split and partition info along with min, max
 * and nullCount stats for each column
 */
public class DeltaLakeGroupScan extends EasyGroupScan {
  private final BatchSchema deltaCommitLogSchema;
  private final boolean scanForAddedPaths;

  public DeltaLakeGroupScan(OpProps props, TableMetadata dataset, BatchSchema deltaCommitLogSchema, List<SchemaPath> columns, boolean scanForAddedPaths) {
    super(props, dataset, columns);
    this.deltaCommitLogSchema = deltaCommitLogSchema;
    this.scanForAddedPaths = scanForAddedPaths;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    final List<SplitAndPartitionInfo> splits = new ArrayList<>(work.size());
    for (SplitWork split : work) {
      if (!scanForAddedPaths && isParquetCheckpointSplit(split)) {
        continue; // Skip parquet checkpoint splits for removed path scans
      }
      splits.add(split.getSplitAndPartitionInfo());
    }
    final List<String> partitionCols = Optional.ofNullable(getDataset().getReadDefinition().getPartitionColumnsList()).orElse(Collections.EMPTY_LIST);
    return new EasySubScan(
      props,
      getDataset().getFormatSettings(),
      splits,
      deltaCommitLogSchema,
      getDataset().getName().getPathComponents(),
      dataset.getStoragePluginId(),
      columns,
      partitionCols,
      getDataset().getReadDefinition().getExtendedProperty());
  }

  private boolean isParquetCheckpointSplit(SplitWork split) {
    try {
      String path = LegacyProtobufSerializer.parseFrom(EasyProtobuf.EasyDatasetSplitXAttr.PARSER,
        split.getSplitAndPartitionInfo().getDatasetSplitInfo().getExtendedProperty().toByteArray()).getPath();
      return path.endsWith("parquet");
    } catch (Exception ignore) {}
    return false;
  }
}
