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
package com.dremio.exec.planner.fragment;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfoList;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/** Helper class to write normalized splits to the rpc, and read the same back. */
public class SplitNormalizer {
  private static final String SPLITS_ATTRIBUTE_KEY = "scan-splits-normalized";
  private static final String PARTITIONS_ATTRIBUTE_KEY = "scan-partitions-normalized";

  // The key must be unique across all the fragments.
  private static String buildPartitionKey(String partitionId) {
    return PARTITIONS_ATTRIBUTE_KEY + "::" + partitionId;
  }

  public static void write(
      OpProps props, MinorDataWriter writer, List<SplitAndPartitionInfo> splits) {
    NormalizedDatasetSplitInfoList.Builder normalizedDatasetSplitInfos =
        NormalizedDatasetSplitInfoList.newBuilder();
    for (SplitAndPartitionInfo split : splits) {
      // write the partition-info.
      assert split.getDatasetSplitInfo().getPartitionId().equals(split.getPartitionInfo().getId());
      writer.writeSplitPartition(
          props, buildPartitionKey(split.getPartitionInfo().getId()), split.getPartitionInfo());

      // collect the split infos into a list.
      normalizedDatasetSplitInfos.addSplits(split.getDatasetSplitInfo());
    }

    // write the collected list of split-infos.
    writer.writeProtoEntry(props, SPLITS_ATTRIBUTE_KEY, normalizedDatasetSplitInfos.build());
  }

  public static List<SplitAndPartitionInfo> read(OpProps props, MinorDataReader reader)
      throws Exception {
    // read the list of split-infos.
    ByteString splitsMsg = reader.readProtoEntry(props, SPLITS_ATTRIBUTE_KEY);
    List<NormalizedDatasetSplitInfo> normalizedSplits =
        NormalizedDatasetSplitInfoList.parseFrom(splitsMsg).getSplitsList();

    // Get the reference to the partition for each split, and build the combined info.
    List<SplitAndPartitionInfo> splits = new ArrayList<>();
    for (NormalizedDatasetSplitInfo splitInfo : normalizedSplits) {
      NormalizedPartitionInfo partitionInfo =
          reader.readSplitPartition(props, buildPartitionKey(splitInfo.getPartitionId()));

      assert splitInfo.getPartitionId().equals(partitionInfo.getId());
      splits.add(new SplitAndPartitionInfo(partitionInfo, splitInfo));
    }
    return splits;
  }
}
