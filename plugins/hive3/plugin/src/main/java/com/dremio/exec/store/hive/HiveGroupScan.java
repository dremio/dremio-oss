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

import java.util.ArrayList;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
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
import com.dremio.service.namespace.dataset.proto.ReadDefinition;

public class HiveGroupScan extends AbstractGroupScan {

  private final ScanFilter filter;

  public HiveGroupScan(
    OpProps props,
    TableMetadata dataset,
    List<SchemaPath> columns,
    ScanFilter filter) {
    super(props, dataset, columns);
    this.filter = filter;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) {
    List<SplitAndPartitionInfo> splits = new ArrayList<>(work.size());
    BatchSchema schema = getDataset().getSchema();
    for(SplitWork split : work){
      splits.add(split.getSplitAndPartitionInfo());
    }
    final ReadDefinition readDefinition = dataset.getReadDefinition();
    final StoragePluginId pluginId = dataset.getStoragePluginId();
    final HiveProxiedSubScan proxiedSubScan = new HiveSubScan(props, splits, schema, dataset.getName().getPathComponents(),
      filter, pluginId, columns, readDefinition.getPartitionColumnsList(), readDefinition.getExtendedProperty().toByteArray());

    return new HiveProxyingSubScan(pluginId.getName(), proxiedSubScan);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }
}
