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
package com.dremio.exec.store.hive;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.hive.exec.HiveSubScan;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.protobuf.ByteString;

public class HiveGroupScan extends AbstractGroupScan {

  private final ScanFilter filter;

  public static final String HIVE_ATTRIBUTE_KEY = "HiveTableXattr";

  public HiveGroupScan(
      TableMetadata dataset,
      List<SchemaPath> columns,
      ScanFilter filter) {
    super(dataset, columns);
    this.filter = filter;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    List<DatasetSplit> splits = new ArrayList<>(work.size());
    BatchSchema schema = getDataset().getSchema();
    for(SplitWork split : work){
      splits.add(split.getSplit());
    }
    final ReadDefinition readDefinition = dataset.getReadDefinition();
    return new HiveSubScan(splits, getUserName(), schema, dataset.getName().getPathComponents(), filter, dataset.getStoragePluginId(), columns,
        readDefinition.getPartitionColumnsList());
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }

  @Override
  public List<Entry<String, ByteString>> getSharedData() {
    return Collections.singletonList(
      new SimpleEntry<>(
        HIVE_ATTRIBUTE_KEY,
        ByteString.copyFrom(dataset.getReadDefinition().getExtendedProperty().asReadOnlyByteBuffer())
      )
    );
  }

}
