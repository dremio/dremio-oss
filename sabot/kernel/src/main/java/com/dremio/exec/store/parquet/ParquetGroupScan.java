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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.utils.ProtostuffUtil;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.AbstractFileGroupScan;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitScanXAttr;
import com.dremio.service.namespace.file.proto.ParquetDatasetSplitXAttr;

import io.protostuff.ByteString;

/**
 * Group scan for file system based tables
 */
public class ParquetGroupScan extends AbstractFileGroupScan {
  private final ParquetScanFilter filter;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final RelDataType cachedRelDataType;

  public ParquetGroupScan(TableMetadata dataset, List<SchemaPath> columns, ParquetScanFilter filter,
                          List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns, RelDataType cachedRelDataType) {
    super(dataset, columns);
    this.filter = filter;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.cachedRelDataType = cachedRelDataType;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    final BatchSchema schema = cachedRelDataType == null ? getDataset().getSchema():  BatchSchema.fromCalciteRowType(cachedRelDataType);

    // Create an abridged version of the splits to save network bytes.
    List<DatasetSplit> splits = work.stream().map(
        workSplit -> ProtostuffUtil.copy(workSplit.getSplit())
            .setExtendedProperty(convertToScanXAttr(workSplit.getSplit().getExtendedProperty()))
    ).collect(Collectors.toList());

    return new ParquetSubScan(dataset.getFormatSettings(), splits, getUserName(), schema,
        getDataset().getName().getPathComponents(), filter == null ? null : filter.getConditions(),
        dataset.getStoragePluginId(), columns, dataset.getReadDefinition().getPartitionColumnsList(),
        globalDictionaryEncodedColumns, dataset.getReadDefinition().getExtendedProperty());
  }

  /*
   * Copy from a full xattr to a scan xattr.
   */
  private ByteString convertToScanXAttr(ByteString xattrFullSerialized) {
    ParquetDatasetSplitXAttr fullXAttr = ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_XATTR_SERIALIZER.revert(xattrFullSerialized.toByteArray());;

    ParquetDatasetSplitScanXAttr scanXAttr = new ParquetDatasetSplitScanXAttr();
    scanXAttr.setPath(fullXAttr.getPath());
    scanXAttr.setFileLength(fullXAttr.getUpdateKey().getLength());
    scanXAttr.setStart(fullXAttr.getStart());
    scanXAttr.setLength(fullXAttr.getLength());
    scanXAttr.setRowGroupIndex(fullXAttr.getRowGroupIndex());
    return ByteString.copyFrom(ParquetDatasetXAttrSerDe.PARQUET_DATASET_SPLIT_SCAN_XATTR_SERIALIZER.serialize(scanXAttr));
  }

  public ParquetScanFilter getFilter() {
    return filter;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

}
