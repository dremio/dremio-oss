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
import java.util.List;
import java.util.UUID;

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
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SampleMutator;
import com.dremio.hbase.proto.HBasePluginProto.HBaseSplitXattr;
import com.dremio.hbase.proto.HBasePluginProto.HBaseTableXattr;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.Affinity;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;


/**
 * Table definition builder for HBase.
 */
public class HBaseTableBuilder implements SourceTableDefinition {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseTableBuilder.class);

  private final NamespaceKey key;
  private final boolean enableRegionCalc;
  private DatasetConfig oldConfig;
  private DatasetConfig dataset;
  private List<DatasetSplit> splits;
  private final HBaseConnectionManager connect;
  private final SabotContext context;

  public HBaseTableBuilder(
      NamespaceKey key,
      DatasetConfig oldConfig,
      HBaseConnectionManager connect,
      boolean enableRegionCalc,
      SabotContext context) {
    super();
    this.key = key;
    this.oldConfig = oldConfig;
    this.connect = connect;
    this.context = context;
    this.enableRegionCalc = enableRegionCalc;
  }

  private void init() throws Exception {
    if(dataset != null) {
      return;
    }

    TableName tableName = TableNameGetter.getTableName(key);

    long count = 0;

    final DatasetConfig datasetConfig;

    final Connection conn = connect.getConnection();
    try (
        Admin admin = conn.getAdmin();
        RegionLocator locator = conn.getRegionLocator(tableName);
        ) {

      final HTableDescriptor table = admin.getTableDescriptor(tableName);

      if(oldConfig != null) {
        datasetConfig = oldConfig;
        final BatchSchema newSchema = getSampledSchema(table, oldConfig);
        datasetConfig.setRecordSchema(newSchema.toByteString());
      } else {
        final BatchSchema schema = getSampledSchema(table, null);
        datasetConfig = new DatasetConfig()
            .setFullPathList(key.getPathComponents())
            .setId(new EntityId(UUID.randomUUID().toString()))
            .setType(DatasetType.PHYSICAL_DATASET)
            .setName(key.getName())
            .setOwner(SystemUser.SYSTEM_USERNAME)
            .setPhysicalDataset(new PhysicalDataset())
            .setRecordSchema(schema.toByteString())
            .setSchemaVersion(DatasetHelper.CURRENT_VERSION);
      }


      final List<HRegionLocation> regionLocations = locator.getAllRegionLocations();

      TableStatsCalculator statsCalculator = new TableStatsCalculator(conn, tableName, context.getConfig(), enableRegionCalc);

      List<DatasetSplit> splits = new ArrayList<>();
      for (HRegionLocation regionLocation : regionLocations) {
        HRegionInfo regionInfo = regionLocation.getRegionInfo();
        long estRowCount = statsCalculator.getRegionSizeInBytes(regionInfo.getRegionName());
        count+= estRowCount;
        splits.add(toSplit(regionLocation.getHostname(), regionInfo, estRowCount));
      }


      final ReadDefinition readDefinition = new ReadDefinition()
          .setExtendedProperty(
              ByteString.copyFrom(
                HBaseTableXattr.newBuilder()
                .setNamespace(com.google.protobuf.ByteString.copyFrom(tableName.getNamespace()))
                .setTable(com.google.protobuf.ByteString.copyFrom(tableName.getName())).build().toByteArray()
              )
          )
          .setScanStats(new ScanStats()
              .setRecordCount(count)
              .setType(ScanStatsType.NO_EXACT_ROW_COUNT)
              .setScanFactor(ScanCostFactor.OTHER.getFactor())
              );

      datasetConfig.setReadDefinition(readDefinition);

      // commit the init.
      this.splits = splits;
      this.dataset = datasetConfig;

    }
  }

  private DatasetSplit toSplit(String hostname, HRegionInfo info, long estimatedRows) {
    DatasetSplit split = new DatasetSplit()
        .setSplitKey(info.getEncodedName())
        .setSize(estimatedRows)
        .setAffinitiesList(ImmutableList.of(
            new Affinity().setFactor((double)estimatedRows).setHost(hostname)
            ))
        .setRowCount(estimatedRows);

    HBaseSplitXattr.Builder xattr = HBaseSplitXattr.newBuilder();
    if(info.getStartKey() != null) {
      xattr.setStart(com.google.protobuf.ByteString.copyFrom(info.getStartKey()));
    }

    if(info.getEndKey() != null) {
      xattr.setStop(com.google.protobuf.ByteString.copyFrom(info.getEndKey()));
    }

    split.setExtendedProperty(ByteString.copyFrom(xattr.build().toByteArray()));
    return split;
  }

  @Override
  public DatasetConfig getDataset() throws Exception {
    init();
    return dataset;
  }

  @Override
  public NamespaceKey getName() {
    return key;
  }

  @Override
  public List<DatasetSplit> getSplits() throws Exception {
    init();
    return splits;
  }

  @Override
  public DatasetType getType() {
    return DatasetType.PHYSICAL_DATASET;
  }

  @Override
  public boolean isSaveable() {
    return true;
  }

  private String getNamespace() {
    return TableNameGetter.getTableName(key).getNamespaceAsString();
  }

  private String getTableName() {
    return key.getName();
  }

  private BatchSchema getSampledSchema(HTableDescriptor descriptor, DatasetConfig oldConfig) throws Exception {

    BatchSchema oldSchema = null;
    ByteString bytes = oldConfig != null ? DatasetHelper.getSchemaBytes(oldConfig) : null;
    if(bytes != null) {
      oldSchema = BatchSchema.deserialize(bytes);
    }

    final HBaseSubScanSpec spec = new HBaseSubScanSpec(getNamespace(), getTableName(), null, null, null);
    try (
        BufferAllocator allocator = context.getAllocator().newChildAllocator("hbase-sample", 0, Long.MAX_VALUE);
        SampleMutator mutator = new SampleMutator(allocator);
        HBaseRecordReader reader = new HBaseRecordReader(connect.getConnection(), spec, GroupScan.ALL_COLUMNS, null, true);
        ) {
      reader.setNumRowsPerBatch(100);

      if(oldSchema != null) {
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
      throw UserException.dataReadError(e).message("Unable to sample schema for table %s.", key).build(logger);
    }
  }

}
