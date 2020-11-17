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
package com.dremio.exec.store.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.mapred.InputSplit;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.hive.exec.HiveProxyingSubScan;
import com.dremio.exec.store.hive.exec.HiveSubScan;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.dremio.hive.proto.HiveReaderProto.HiveSplitXattr;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Preconditions;

import io.protostuff.ByteStringUtil;

public class HiveGroupScan extends AbstractGroupScan {

  private final ScanFilter filter;
  private final long orcAcidDeltasLimit;

  public HiveGroupScan(
    OpProps props,
    TableMetadata dataset,
    List<SchemaPath> columns,
    ScanFilter filter,
    QueryContext context) {
    super(props, dataset, columns);
    this.filter = filter;
    this.orcAcidDeltasLimit = context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX) *
      context.getOptions().getOption(CatalogOptions.ORC_DELTA_LEAF_COLUMN_FACTOR);
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) {
    List<SplitAndPartitionInfo> splits = new ArrayList<>(work.size());
    BatchSchema schema = getDataset().getSchema();
    for(SplitWork split : work){
      try {
        // With ORC ACID, the effective number of readers is the product of the number of deltas and
        // the number of columns. These readers use heap memory and if the value of this product
        // is too high, it can overwhelm executor nodes and cause a heap outage.
        InputSplit inputSplit = HiveUtilities.deserializeInputSplit(
          HiveSplitXattr.parseFrom(
            split.getDatasetSplit().getSplitExtendedProperty())
            .getInputSplit());
        if (inputSplit instanceof OrcSplit) {
          OrcSplit orcSplit = (OrcSplit) inputSplit;
          Preconditions.checkState(orcSplit.getDeltas() != null, "Unexpected state");
          if (orcSplit.getDeltas().size() * columns.size() > this.orcAcidDeltasLimit) {
            throw UserException.validationError().message(
              "Too many delta files exist for the ORC ACID table, %s. Please run Hive compaction on the table and try again after metadata refresh.", getDataset().getName()).buildSilently();
          }
        }
      } catch (IOException | ReflectiveOperationException e) {
        //e.printStackTrace();
      }

      splits.add(split.getSplitAndPartitionInfo());
    }
    final ReadDefinition readDefinition = dataset.getReadDefinition();
    final StoragePluginId pluginId = dataset.getStoragePluginId();
    final HiveProxiedSubScan proxiedSubScan = new HiveSubScan(props, splits, schema, dataset.getName().getPathComponents(),
      filter, pluginId, columns, readDefinition.getPartitionColumnsList(), ByteStringUtil.unwrap(readDefinition.getExtendedProperty()));

    return new HiveProxyingSubScan(pluginId, proxiedSubScan);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }
}
