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
package com.dremio.exec.store.dfs.easy;

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.file.proto.FileType;

/**
 * New easy group scan
 */
public class EasyGroupScan extends AbstractGroupScan {

  public EasyGroupScan(
      OpProps props,
      TableMetadata dataset,
      List<SchemaPath> columns) {
    super(props, dataset, columns);
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    final List<SplitAndPartitionInfo> splits = new ArrayList<>(work.size());
    final BatchSchema fullSchema = getDataset().getSchema();
    for(SplitWork split : work){
      splits.add(split.getSplitAndPartitionInfo());
    }

    return new EasySubScan(
        props,
        getDataset().getFormatSettings(),
        splits,
        fullSchema,
        getDataset().getName().getPathComponents(),
        dataset.getStoragePluginId(),
        columns,
        getDataset().getReadDefinition().getPartitionColumnsList(),
        getDataset().getReadDefinition().getExtendedProperty());
  }


  @Override
  public int getOperatorType() {
    return getEasyScanOperatorType(dataset.getFormatSettings().getType());
  }

  public static int getEasyScanOperatorType(FileType datasetType) {
    switch (datasetType) {
      case JSON:
        return UserBitShared.CoreOperatorType.JSON_SUB_SCAN_VALUE;

      case TEXT:
      case CSV:
      case TSV:
      case PSV:
        return UserBitShared.CoreOperatorType.TEXT_SUB_SCAN_VALUE;

      case PARQUET:
        return UserBitShared.CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;

      case EXCEL:
      case XLS:
        return UserBitShared.CoreOperatorType.EXCEL_SUB_SCAN_VALUE;

      case ARROW:
        return UserBitShared.CoreOperatorType.ARROW_SUB_SCAN_VALUE;
      case ICEBERG:
        return UserBitShared.CoreOperatorType.ICEBERG_SUB_SCAN_VALUE;
      case UNKNOWN:
      case HTTP_LOG:
      default:
        throw new UnsupportedOperationException("format not supported " + datasetType);
    }
  }

}
