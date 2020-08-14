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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitScanXAttr;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfo;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Group scan for file system based tables
 */
public class ParquetGroupScan extends AbstractGroupScan {

  private final ParquetScanFilter filter;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final RelDataType cachedRelDataType;
  private final boolean arrowCachingEnabled;

  public ParquetGroupScan(
    OpProps props,
    TableMetadata dataset,
    List<SchemaPath> columns,
    ParquetScanFilter filter,
    List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
    RelDataType cachedRelDataType,
    boolean arrowCachingEnabled) {
    super(props, dataset, columns);
    this.filter = filter;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.cachedRelDataType = cachedRelDataType;
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) {
    final BatchSchema schema = cachedRelDataType == null ? getDataset().getSchema():  CalciteArrowHelper.fromCalciteRowType(cachedRelDataType);

    List<SplitAndPartitionInfo> splits = work.stream()
        .map(SplitWork::getSplitAndPartitionInfo)
        .map(split -> {
          // Create an abridged version of the splits to save network bytes.
          // NOTE: probably not a good idea to reuse an opaque field to store 2 different objects
          final ParquetDatasetSplitXAttr fullXAttr;
          try {
            fullXAttr = LegacyProtobufSerializer.parseFrom(ParquetDatasetSplitXAttr.PARSER,
              split.getDatasetSplitInfo().getExtendedProperty());
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Could not deserialize Parquet dataset split info", e);
          }
          return new SplitAndPartitionInfo(split.getPartitionInfo(),
              NormalizedDatasetSplitInfo.newBuilder(split.getDatasetSplitInfo())
                .setExtendedProperty(convertToScanXAttr(fullXAttr).toByteString())
                .build());
        })
        .collect(Collectors.toList());

    return new ParquetSubScan(
        getProps(),
        dataset.getFormatSettings(),
        splits,
        schema,
        ImmutableList.of(getDataset().getName().getPathComponents()),
        filter == null ? null : filter.getConditions(),
        dataset.getStoragePluginId(), columns, dataset.getReadDefinition().getPartitionColumnsList(),
        globalDictionaryEncodedColumns, dataset.getReadDefinition().getExtendedProperty(),
      arrowCachingEnabled);
  }

  /*
   * Copy from a full xattr to a scan xattr.
   */
  private ParquetDatasetSplitScanXAttr convertToScanXAttr(ParquetDatasetSplitXAttr fullXAttr) {
    return ParquetDatasetSplitScanXAttr.newBuilder()
        .setPath(fullXAttr.getPath())
        .setFileLength(fullXAttr.getUpdateKey().getLength())
        .setStart(fullXAttr.getStart())
        .setLength(fullXAttr.getLength())
        .setLastModificationTime(fullXAttr.getUpdateKey().getLastModificationTime())
        .setRowGroupIndex(fullXAttr.getRowGroupIndex())
        .build();
  }

  public ParquetScanFilter getFilter() {
    return filter;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

}
