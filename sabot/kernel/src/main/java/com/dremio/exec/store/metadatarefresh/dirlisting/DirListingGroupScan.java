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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.metadatarefresh.RefreshExecTableMetadata;

public class DirListingGroupScan extends AbstractGroupScan {

  private final StoragePluginId storagePluginId;
  private final BatchSchema schema;
  private final boolean allowRecursiveListing;

  public DirListingGroupScan(OpProps props, TableMetadata dataset, BatchSchema schema, List<SchemaPath> columns, StoragePluginId pluginId, boolean allowRecursiveListing) {
      super(props, dataset, columns);
      this.storagePluginId = pluginId;
      this.schema = schema;
      this.allowRecursiveListing = allowRecursiveListing;
  }

  @Override
  public int getMaxParallelizationWidth() {
      return dataset.getSplitCount();
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
      final List<SplitAndPartitionInfo> splits = work.stream().map(SplitWork::getSplitAndPartitionInfo)
              .collect(Collectors.toList());
      return new DirListingSubScan(props, splits, getReferencedTables(), columns, schema, getTableSchema(), storagePluginId, /* TODO: For Hive plugin make this configurable */allowRecursiveListing);
  }

  @Override
  public int getOperatorType() {
      return UserBitShared.CoreOperatorType.DIR_LISTING_SUB_SCAN_VALUE;
  }

  private BatchSchema getTableSchema() {
    if (dataset instanceof RefreshExecTableMetadata) {
      return ((RefreshExecTableMetadata) dataset).getTableSchema();
    }
    return dataset.getSchema();
  }
}
