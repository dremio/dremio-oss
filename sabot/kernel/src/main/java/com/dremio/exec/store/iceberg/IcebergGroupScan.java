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

import static com.dremio.exec.store.iceberg.IcebergUtils.getMetadataLocation;
import static com.dremio.exec.store.iceberg.IcebergUtils.getSplitAndPartitionInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.iceberg.expressions.Expression;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.SplitWorkWithRuntimeAffinity;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.easy.EasyGroupScan;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;

import io.protostuff.ByteString;

/**
 * Iceberg dataset group scan
 */
public class IcebergGroupScan extends EasyGroupScan {

  private final Expression icebergFilterExpression;
  private final ManifestContentType manifestContent;

  public IcebergGroupScan(OpProps props, TableMetadata dataset, List<SchemaPath> columns,
      Expression icebergFilterExpression, ManifestContentType manifestContent) {
    super(props, dataset, columns);
    this.icebergFilterExpression = icebergFilterExpression;
    this.manifestContent = manifestContent;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> works) throws ExecutionSetupException {

    final StoragePluginId pluginId;
    if (dataset instanceof InternalIcebergScanTableMetadata) {
      InternalIcebergScanTableMetadata icebergDataset = (InternalIcebergScanTableMetadata) dataset;
      pluginId = icebergDataset.getIcebergTableStoragePlugin();
    } else {
      pluginId = dataset.getStoragePluginId();
    }

    final String metadataLocation = getMetadataLocation(dataset, works);
    final IcebergExtendedProp icebergExtendedProp;
    try {
      ByteString partitionSpecMap = null;
      long snapshotId = -1;
      String schema = null;

      final IcebergMetadata icebergMetadata = getDataset().getDatasetConfig().getPhysicalDataset().getIcebergMetadata();
      if (icebergMetadata != null) {
        partitionSpecMap = icebergMetadata.getPartitionSpecs() == null ? icebergMetadata.getPartitionSpecsJsonMap(): icebergMetadata.getPartitionSpecs() ;
        snapshotId = icebergMetadata.getSnapshotId() == null ? -1 : icebergMetadata.getSnapshotId();
        schema = icebergMetadata.getJsonSchema();
      }
      icebergExtendedProp = new IcebergExtendedProp(
          partitionSpecMap,
          IcebergSerDe.serializeToByteArray(icebergFilterExpression),
          snapshotId,
          schema
      );
    } catch (IOException e) {
      throw new RuntimeException("Unable to serialized iceberg filter expression");
    }

    return new IcebergManifestListSubScan(
      props,
      metadataLocation,
      props.getSchema(),
      getSplitAndPartitionInfo(metadataLocation),
      getDataset().getName().getPathComponents(),
      pluginId,
      dataset.getStoragePluginId(),
      columns,
      getDataset().getReadDefinition().getPartitionColumnsList(),
      getDataset().getReadDefinition().getExtendedProperty(),
      icebergExtendedProp,
      manifestContent);
  }

  @Override
  public Iterator<SplitWork> getSplits(ExecutionNodeMap nodeMap) {
    return SplitWorkWithRuntimeAffinity.transform(dataset.getSplits(), nodeMap, getDistributionAffinity());
  }
}
