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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpointList;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
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
  private final SharedAttrsIndex.Builder sharedAttrsBuilder;
  private final MinorDataSerDe serDe;
  private final List<MinorAttr> attrList = new ArrayList<>();
  private final Set<String> keySet = new HashSet<>();

  public MinorDataWriter(
    FragmentHandle handle,
    NodeEndpoint endpoint,
    MinorDataSerDe serDe,
    PlanFragmentsIndex.Builder indexBuilder) {

    this.handle = handle;
    this.serDe = serDe;
    this.sharedAttrsBuilder = indexBuilder.getSharedAttrsIndexBuilder(endpoint);
  }

  public List<MinorAttr> getAllAttrs() {
    return attrList;
  }

  /*
   * Add a new (key, value) pair. The key consists of the operatorId and an arbitrary string.
   */
  private void addEntry(OpProps props, String key, ByteString value) {
    int operatorId = props.getOperatorId();

    Preconditions.checkState(!keySet.contains(operatorId + key),
      "duplicate attribute operatorId " + operatorId + " key " + key + " in fragment " + handle);
    keySet.add(operatorId + key);

    attrList.add(MinorAttr.newBuilder()
      .setName(key)
      .setValue(value)
      .setOperatorId(operatorId)
      .build());
  }

  public void writeMinorFragmentIndexEndpoint(OpProps props, String key, MinorFragmentIndexEndpoint endpoint) {
    addEntry(props, key, serDe.serialize(endpoint));
  }

  public void writeMinorFragmentIndexEndpoints(OpProps props, String key, List<MinorFragmentIndexEndpoint> endpoints) {
    MinorFragmentIndexEndpointList indexEndpointList =
      MinorFragmentIndexEndpointList
        .newBuilder()
        .addAllFrags(endpoints)
        .build();

    addEntry(props, key, serDe.serialize(indexEndpointList));
  }

  // Serialize protobuf message, and save as a kv pair.
  public void writeProtoEntry(OpProps props, String key, MessageLite msg) {
    addEntry(props, key, serDe.serialize(msg));
  }

  // Serialize pojo to compressed json, and save as kv pair.
  public void writeJsonEntry(OpProps props, String key, Object object) throws JsonProcessingException {
   addEntry(props, key, serDe.serializeObjectToJson(object));
  }

  // Serialize split partition, and save as a kv pair in the shared attributes index.
  public void writeSplitPartition(OpProps props, String key, NormalizedPartitionInfo partitionInfo) {
    sharedAttrsBuilder.addAttr(props, key, () -> serDe.serialize(partitionInfo));
  }

}
