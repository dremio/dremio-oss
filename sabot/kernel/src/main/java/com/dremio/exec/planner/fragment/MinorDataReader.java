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
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;

/** Utility class to deserialize and read minor-specific data. */
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

  public MinorFragmentIndexEndpoint readMinorFragmentIndexEndpoint(OpProps props, String key)
      throws Exception {
    ByteString buffer = attrsMap.getAttrValue(props, key);
    return serDe.deserializeMinorFragmentIndexEndpoint(buffer);
  }

  public List<MinorFragmentIndexEndpoint> readMinorFragmentIndexEndpoints(OpProps props, String key)
      throws Exception {
    ByteString buffer = attrsMap.getAttrValue(props, key);
    return serDe.deserializeMinorFragmentIndexEndpointList(buffer).getFragsList();
  }

  public ByteString readProtoEntry(OpProps props, String key) throws Exception {
    return attrsMap.getAttrValue(props, key);
  }

  /*
   * Partition infos are shared attributes (de-deuped at the rpc level).
   */
  public NormalizedPartitionInfo readSplitPartition(OpProps props, String key) throws Exception {
    return index.getSharedAttrsIndex().getSplitPartitionEntry(props, key);
  }

  /*
   * Deserialize pojo from compressed json extracted from the value part of the kv pair.
   */
  public <T> T readJsonEntry(OpProps props, String key, Class<T> clazz) throws IOException {
    ByteString buffer = attrsMap.getAttrValue(props, key);
    return serDe.deserializeObjectFromJson(clazz, buffer);
  }
}
