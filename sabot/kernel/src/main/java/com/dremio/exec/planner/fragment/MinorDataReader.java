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

import java.io.IOException;
import java.util.List;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfo;
import com.google.protobuf.ByteString;

/**
 * Utility class to deserialize and read minor-specific data.
 */
public class MinorDataReader {
  private final FragmentHandle handle;
  private final PlanFragmentsIndex index;
  private final MinorDataSerDe serDe;
  private final MinorAttrsMap attrsMap;

  public MinorDataReader(
    FragmentHandle handle,
    MinorDataSerDe serDe,
    PlanFragmentsIndex index,
    MinorAttrsMap attrsMap) {

    this.handle = handle;
    this.serDe = serDe;
    this.index = index;
    this.attrsMap = attrsMap;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  public List<SplitInfo> readSplits(PhysicalOperator op, String key) throws Exception {
    ByteString buffer = attrsMap.getMinorAttrsValue(op, key);
    return serDe.deserializeSplits(buffer);
  }

  public MinorFragmentIndexEndpoint readMinorFragmentIndexEndpoint(PhysicalOperator op, String key) throws Exception {
    ByteString buffer = attrsMap.getMinorAttrsValue(op, key);
    return serDe.deserializeMinorFragmentIndexEndpoint(buffer);
  }

  public List<MinorFragmentIndexEndpoint> readMinorFragmentIndexEndpoints(PhysicalOperator op, String key) throws Exception {
    ByteString buffer = attrsMap.getMinorAttrsValue(op, key);
    return serDe.deserializeMinorFragmentIndexEndpointList(buffer).getFragsList();
  }

  public ByteString readProtoEntry(PhysicalOperator op, String key) throws Exception {
    return attrsMap.getMinorAttrsValue(op, key);
  }

  /*
   * Deserialize pojo from compressed json extracted from the value part of the kv pair.
   */
  public <T> T readJsonEntry(PhysicalOperator op, String key, Class<T> clazz) throws IOException {
    ByteString buffer = attrsMap.getMinorAttrsValue(op, key);
    return serDe.deserializeObjectFromJson(clazz, buffer);
  }
}
