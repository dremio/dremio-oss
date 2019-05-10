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
package com.dremio.exec.store.hbase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetSplitAffinity;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.proto.CoordExecRPC.HBaseSubScanSpec;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SampleMutator;
import com.dremio.hbase.proto.HBasePluginProto.HBaseSplitXattr;
import com.dremio.hbase.proto.HBasePluginProto.HBaseTableXattr;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;


/**
 * Table definition builder for HBase.
 */
public class HBaseTableBuilder implements DatasetHandle {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseTableBuilder.class);

  private final EntityPath entityPath;
  private final HBaseConnectionManager connect;
  private final boolean enableRegionCalc;
  private final SabotContext context;

  private DatasetMetadata datasetMetadata;
  private List<PartitionChunk> partitionChunks;

  HBaseTableBuilder(
      EntityPath entityPath,
      HBaseConnectionManager connect,
      boolean enableRegionCalc,
      SabotContext context
  ) {
    this.entityPath = entityPath;
    this.connect = connect;
    this.context = context;
    this.enableRegionCalc = enableRegionCalc;
  }

  private void buildIfNecessary(BatchSchema oldSchema) throws ConnectorException {
    if (partitionChunks != null) {
      return;
    }

    long recordCount = 0;
    final BatchSchema schema;
    final List<PartitionChunk> chunks = new ArrayList<>();

    final TableName tableName = TableNameGetter.getTableName(entityPath);
    final Connection conn = connect.getConnection();
    try (Admin admin = conn.getAdmin();
         RegionLocator locator = conn.getRegionLocator(tableName)) {

      final HTableDescriptor table = admin.getTableDescriptor(tableName);
      final List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
      final TableStatsCalculator statsCalculator = new TableStatsCalculator(conn, tableName, context.getConfig(),
          enableRegionCalc);
      schema = getSampledSchema(table, oldSchema);

      for (HRegionLocation regionLocation : regionLocations) {
        HRegionInfo regionInfo = regionLocation.getRegionInfo();
        long estRowCount = statsCalculator.getRegionSizeInBytes(regionInfo.getRegionName());
        recordCount += estRowCount;
        chunks.add(toPartitionChunk(regionLocation.getHostname(), regionInfo, estRowCount));
      }
    } catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new ConnectorException(e);
    }

    final HBaseTableXattr extended = HBaseTableXattr.newBuilder()
        .setNamespace(ByteString.copyFrom(tableName.getNamespace()))
        .setTable(ByteString.copyFrom(tableName.getName()))
        .build();

    datasetMetadata = DatasetMetadata.of(DatasetStats.of(recordCount, ScanCostFactor.OTHER.getFactor()), schema,
        extended::writeTo);
    partitionChunks = chunks;
  }

  private static PartitionChunk toPartitionChunk(String hostname, HRegionInfo info, long estimatedRows) {
    final HBaseSplitXattr.Builder xattr = HBaseSplitXattr.newBuilder();
    if (info.getStartKey() != null) {
      xattr.setStart(ByteString.copyFrom(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      xattr.setStop(ByteString.copyFrom(info.getEndKey()));
    }
    final HBaseSplitXattr extended = xattr.build();

    final DatasetSplitAffinity splitAffinity = DatasetSplitAffinity.of(hostname, estimatedRows);
    final DatasetSplit datasetSplit = DatasetSplit.of(Collections.singletonList(splitAffinity), estimatedRows,
        estimatedRows, extended::writeTo);
    return PartitionChunk.of(datasetSplit);
  }

  private String getNamespace() {
    return TableNameGetter.getTableName(entityPath).getNamespaceAsString();
  }

  private String getTableName() {
    return entityPath.getName();
  }

  private BatchSchema getSampledSchema(HTableDescriptor descriptor, BatchSchema oldSchema) throws Exception {

    final HBaseSubScanSpec spec = HBaseSubScanSpec.newBuilder()
      .setNamespace(getNamespace())
      .setTableName(getTableName())
      .build();
    try (
        BufferAllocator allocator = context.getAllocator().newChildAllocator("hbase-sample", 0, Long.MAX_VALUE);
        SampleMutator mutator = new SampleMutator(allocator);
        HBaseRecordReader reader = new HBaseRecordReader(connect.getConnection(), spec, GroupScan.ALL_COLUMNS, null, true)
    ) {
      reader.setNumRowsPerBatch(100);

      if (oldSchema != null) {
        oldSchema.materializeVectors(GroupScan.ALL_COLUMNS, mutator);
      }

      // add row key.
      mutator.addField(CompleteType.VARBINARY.toField(HBaseRecordReader.ROW_KEY), ValueVector.class);

      // add all column families.
      for (HColumnDescriptor col : descriptor.getFamilies()) {
        mutator.addField(CompleteType.struct().toField(col.getNameAsString()), ValueVector.class);
      }

      reader.setup(mutator);
      reader.next();
      mutator.getContainer().buildSchema(SelectionVectorMode.NONE);
      return mutator.getContainer().getSchema();
    } catch (ExecutionSetupException e) {
      throw UserException.dataReadError(e).message("Unable to sample schema for table %s.", entityPath).build(logger);
    }
  }

  @Override
  public EntityPath getDatasetPath() {
    return entityPath;
  }

  DatasetMetadata getDatasetMetadata(BatchSchema oldSchema) throws ConnectorException {
    buildIfNecessary(oldSchema);

    return datasetMetadata;
  }

  PartitionChunkListing listPartitionChunks(BatchSchema oldSchema) throws ConnectorException {
    buildIfNecessary(oldSchema);

    return () -> partitionChunks.iterator();
  }
}
