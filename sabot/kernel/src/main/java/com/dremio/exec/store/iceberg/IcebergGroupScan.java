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
package com.dremio.exec.store.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.iceberg.expressions.Expression;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.SplitWorkWithRuntimeAffinity;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.easy.EasyGroupScan;
import com.dremio.exec.store.dfs.easy.EasySubScan;

import io.protostuff.ByteString;

/**
 * Iceberg dataset group scan
 */
public class IcebergGroupScan extends EasyGroupScan {
  private final Expression icebergFilterExpression;
  public IcebergGroupScan(OpProps props, TableMetadata dataset, List<SchemaPath> columns, Expression icebergFilterExpression) {
    super(props, dataset, columns);
    this.icebergFilterExpression = icebergFilterExpression;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {

    List<SplitAndPartitionInfo> splits = new ArrayList<>();
    StoragePluginId pluginId;
    if (dataset instanceof InternalIcebergScanTableMetadata) {
      InternalIcebergScanTableMetadata icebergDataset = (InternalIcebergScanTableMetadata) dataset;
      splits = icebergDataset.getSplitAndPartitionInfo();
      pluginId = icebergDataset.getIcebergTableStoragePlugin();
    } else {
      for (SplitWork split : work) {
        splits.add(split.getSplitAndPartitionInfo());
      }
      pluginId = dataset.getStoragePluginId();
    }
    IcebergExtendedProp icebergExtendedProp = null;
    try {
      ByteString partitionSpecMap = null;
      if(getDataset().getDatasetConfig().getPhysicalDataset().getIcebergMetadata() != null){
        partitionSpecMap = getDataset().getDatasetConfig().getPhysicalDataset().getIcebergMetadata().getPartitionSpecs();
      }
      icebergExtendedProp = new IcebergExtendedProp(partitionSpecMap, IcebergSerDe.serializeToByteArray(icebergFilterExpression));
    } catch (IOException e) {
      throw new RuntimeException("Unabled to serialized iceberg filter expression");
    }

    return new EasySubScan(
      props,
      getDataset().getFormatSettings(),
      splits,
      props.getSchema(),
      getDataset().getName().getPathComponents(),
      pluginId,
      dataset.getStoragePluginId(),
      columns,
      getDataset().getReadDefinition().getPartitionColumnsList(),
      getDataset().getReadDefinition().getExtendedProperty(),
      icebergExtendedProp);
  }

  @Override
  public Iterator<SplitWork> getSplits(ExecutionNodeMap nodeMap) {
    return SplitWorkWithRuntimeAffinity.transform(dataset.getSplits(), nodeMap, getDistributionAffinity());
  }
}
