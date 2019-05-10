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

import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.FragmentCodec;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpointList;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfo;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.SplitInfoList;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;


public class MinorDataSerDe {
  private final PhysicalPlanReader reader;
  private final FragmentCodec codec;

  public MinorDataSerDe(PhysicalPlanReader reader, FragmentCodec codec) {
    this.reader = reader;
    this.codec = codec;
  }

  public ByteString serialize(MessageLite msg) {
    return msg.toByteString();
  }

  public List<SplitInfo> deserializeSplits(ByteString buffer) throws InvalidProtocolBufferException {
    return SplitInfoList.parseFrom(buffer).getSplitsList();
  }

  public MinorFragmentIndexEndpoint deserializeMinorFragmentIndexEndpoint(ByteString buffer) throws InvalidProtocolBufferException {
    return MinorFragmentIndexEndpoint.parseFrom(buffer);
  }

  public MinorFragmentIndexEndpointList deserializeMinorFragmentIndexEndpointList(ByteString buffer) throws InvalidProtocolBufferException {
    return MinorFragmentIndexEndpointList.parseFrom(buffer);
  }

  public ByteString serializeObjectToJson(Object object) throws JsonProcessingException {
    return reader.writeObject(object, codec);
  }

  public <T> T deserializeObjectFromJson(Class<T> clazz, ByteString buffer) throws IOException {
    return reader.readObject(clazz, buffer, codec);
  }

}
