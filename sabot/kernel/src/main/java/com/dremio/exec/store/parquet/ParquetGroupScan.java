/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.AbstractFileGroupScan;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;

/**
 * Group scan for file system based tables
 */
public class ParquetGroupScan extends AbstractFileGroupScan {
  private final List<FilterCondition> conditions;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final RelDataType cachedRelDataType;

  public ParquetGroupScan(TableMetadata dataset, List<SchemaPath> columns, List<FilterCondition> conditions,
                          List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns, RelDataType cachedRelDataType) {
    super(dataset, columns);
    this.conditions = conditions;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.cachedRelDataType = cachedRelDataType;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    final List<DatasetSplit> splits = new ArrayList<>(work.size());
    final BatchSchema schema = cachedRelDataType == null ? getDataset().getSchema():  BatchSchema.fromCalciteRowType(cachedRelDataType);
    for(SplitWork split : work){
      splits.add(split.getSplit());
    }
    return new ParquetSubScan(dataset.getFormatSettings(), splits, getUserName(), schema, getDataset().getName().getPathComponents(), conditions,
      dataset.getStoragePluginId(), columns, dataset.getReadDefinition().getPartitionColumnsList(), globalDictionaryEncodedColumns, dataset.getReadDefinition().getExtendedProperty());
  }

  public List<GlobalDictionaryFieldInfo> getGlobalDictionaryEncodedColumns() {
    return globalDictionaryEncodedColumns;
  }

  public List<FilterCondition> getConditions() {
    return conditions;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

}
