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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedDatasetSplitInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.google.protobuf.ByteString;

public class TestSplitNormalizer {
  NodeEndpoint dummyEndpoint0 = NodeEndpoint
    .newBuilder()
    .setAddress("test0")
    .build();

  NodeEndpoint dummyEndpoint1 = NodeEndpoint
    .newBuilder()
    .setAddress("test1")
    .build();

  private List<SplitAndPartitionInfo> buildSplits(int numPartitions, int numSplitsPerPartition) {
    List<SplitAndPartitionInfo> splitAndPartitionInfos = new ArrayList<>();

    for (int i = 0; i < numPartitions; ++i) {
      String extendedProp = "partition_extended_" + String.valueOf(i);
      NormalizedPartitionInfo partitionInfo = NormalizedPartitionInfo
        .newBuilder()
        .setId(String.valueOf(i))
        .setSize(i * 1000)
        .setExtendedProperty(ByteString.copyFrom(extendedProp.getBytes()))
        .build();

      for (int j = 0; j < numSplitsPerPartition; ++j) {
        String splitExtendedProp = "split_extended_" + String.valueOf(j);

        NormalizedDatasetSplitInfo splitInfo = NormalizedDatasetSplitInfo
          .newBuilder()
          .setPartitionId(String.valueOf(i))
          .setExtendedProperty(ByteString.copyFrom(splitExtendedProp.getBytes()))
          .build();

        splitAndPartitionInfos.add(new SplitAndPartitionInfo(partitionInfo, splitInfo));
      }
    }
    return splitAndPartitionInfos;
  }

  @Test
  public void singleNode() throws Exception {
    MinorDataSerDe serDe = new MinorDataSerDe(null, null);


    // write the split infos.
    List<SplitAndPartitionInfo> writeSplits = buildSplits(5, 10);
    PlanFragmentsIndex.Builder indexBuilder = new PlanFragmentsIndex.Builder();
    MinorDataWriter writer = new MinorDataWriter(null, dummyEndpoint0, serDe, indexBuilder);
    SplitNormalizer.write(OpProps.prototype(1), writer, writeSplits);


    // read the split infos.
    List<MinorAttr> sharedAttrs = indexBuilder.getSharedAttrsIndexBuilder(dummyEndpoint0).getAllAttrs();
    PlanFragmentsIndex index = new PlanFragmentsIndex(
      indexBuilder.endpointsIndexBuilder.getAllEndpoints(), sharedAttrs);
    assertEquals(5, sharedAttrs.size());

    MinorAttrsMap minorAttrsMap = MinorAttrsMap.create(writer.getAllAttrs());
    MinorDataReader reader = new MinorDataReader(null, serDe, index, minorAttrsMap);
    List<SplitAndPartitionInfo> readSplits = SplitNormalizer.read(OpProps.prototype(1), reader);

    assertArrayEquals(writeSplits.toArray(), readSplits.toArray());
  }

  @Test
  public void multiNode() throws Exception {
    MinorDataSerDe serDe = new MinorDataSerDe(null, null);

    List<SplitAndPartitionInfo> writeSplits = buildSplits(5, 10);

    NodeEndpoint[] endpoints = {dummyEndpoint0, dummyEndpoint1};

    // assign alternate splits to each node.
    List<SplitAndPartitionInfo>[] writeSplitsSharded = new List[2];
    writeSplitsSharded[0] = writeSplitsSharded[1] = new ArrayList<>();
    for (int i = 0; i < writeSplits.size(); i++) {
      if (i % 2 == 0) {
        writeSplitsSharded[0].add(writeSplits.get(i));
      } else {
        writeSplitsSharded[1].add(writeSplits.get(i));
      }
    }

    // write the split infos.
    PlanFragmentsIndex.Builder indexBuilder = new PlanFragmentsIndex.Builder();
    MinorDataWriter[] minorDataWriters = new MinorDataWriter[2];
    for (int i = 0; i < 2; ++i) {
      minorDataWriters[i] = new MinorDataWriter(null, endpoints[i], serDe, indexBuilder);
      SplitNormalizer.write(OpProps.prototype(1), minorDataWriters[i], writeSplitsSharded[i]);
    }

    // read the split infos.
    for (int i = 0; i < 2; ++i) {

      List<MinorAttr> sharedAttrs = indexBuilder.getSharedAttrsIndexBuilder(endpoints[i]).getAllAttrs();
      PlanFragmentsIndex index =
          new PlanFragmentsIndex(indexBuilder.endpointsIndexBuilder.getAllEndpoints(), sharedAttrs);
      assertEquals(5, sharedAttrs.size());

      MinorAttrsMap minorAttrsMap = MinorAttrsMap.create(minorDataWriters[i].getAllAttrs());
      MinorDataReader reader = new MinorDataReader(null, serDe, index, minorAttrsMap);
      List<SplitAndPartitionInfo> readSplits = SplitNormalizer.read(OpProps.prototype(1), reader);

      assertArrayEquals(writeSplitsSharded[i].toArray(), readSplits.toArray());
    }
  }
}
