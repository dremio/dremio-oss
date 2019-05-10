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
package com.dremio.exec.planner.fragment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpointList;
import com.dremio.exec.proto.CoordExecRPC.MinorSpecificAttr;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfoList;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.MessageLite;

/**
 * Utility class to serialize and gather all minor-specific data.
 * - The attributes are stored as key, value pairs in either compressed-json or protobuf format.
 * - Repetitive entries like end-points are saved in the indexBuilder, and the index value is
 *   stored in the key-value pair.
 */
public class MinorDataWriter {
  private final FragmentHandle handle;
  private final PlanFragmentsIndex.Builder indexBuilder;
  private final MinorDataSerDe serDe;
  private final List<MinorSpecificAttr> attrList = new ArrayList<>();
  private final Set<String> keySet = new HashSet<>();

  public MinorDataWriter(
    FragmentHandle handle,
    MinorDataSerDe serDe,
    PlanFragmentsIndex.Builder indexBuilder) {

    this.handle = handle;
    this.serDe = serDe;
    this.indexBuilder = indexBuilder;
  }

  public List<MinorSpecificAttr> getAttrList() {
    return attrList;
  }

  /*
   * Add a new (key, value) pair. The key consists of the operatorId and an arbitrary string.
   */
  private void addEntry(PhysicalOperator op, String key, ByteString value) {
    int operatorId = op.getProps().getLocalOperatorId();

    assert !keySet.contains(operatorId + key) :
      "duplicate attribute operatorId " + operatorId + " key " + key + " in fragment " + handle;
    keySet.add(operatorId + key);

    attrList.add(MinorSpecificAttr.newBuilder()
      .setName(key)
      .setValue(value)
      .setOperatorId(op.getProps().getLocalOperatorId())
      .build());
  }

  /*
   * serialize split info as protobuf and save as kv pair.
   */
  public void writeSplits(PhysicalOperator op, String key, List<SplitInfo> splits) {
    // Trim the splits to eliminate the attributes that are not required by executors.
    List<SplitInfo> trimmed =
      splits
        .stream()
        .map(x -> SplitInfo.newBuilder(x).clearAffinities().build())
        .collect(Collectors.toList());

    SplitInfoList list =
      SplitInfoList.newBuilder()
        .addAllSplits(trimmed)
        .build();

    addEntry(op, key, serDe.serialize(list));
  }

  public void writeMinorFragmentIndexEndpoint(PhysicalOperator op, String key, MinorFragmentIndexEndpoint endpoint) {
    addEntry(op, key, serDe.serialize(endpoint));
  }

  public void writeMinorFragmentIndexEndpoints(PhysicalOperator op, String key, List<MinorFragmentIndexEndpoint> endpoints) {
    MinorFragmentIndexEndpointList indexEndpointList =
      MinorFragmentIndexEndpointList
        .newBuilder()
        .addAllFrags(endpoints)
        .build();

    addEntry(op, key, serDe.serialize(indexEndpointList));
  }

  /*
   * Serialize protobuf message, and save as a kv pair.
   */
  public void writeProtoEntry(PhysicalOperator op, String key, MessageLite msg) {
    addEntry(op, key, serDe.serialize(msg));
  }

  /*
   * Serialize pojo to compressed json, and save as kv pair.
   */
  public void writeJsonEntry(PhysicalOperator op, String key, Object object) throws JsonProcessingException {
   addEntry(op, key, serDe.serializeObjectToJson(object));
  }

}
