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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.fragment.MinorAttrsMap.Key;
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.NormalizedPartitionInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Index for attributes that are shared across all fragments in the plan.
 */
public class SharedAttrsIndex {
  MinorAttrsMap attrMap;
  Map<Key, NormalizedPartitionInfo> partitionInfoMap;

  private SharedAttrsIndex(List<MinorAttr> attrs) {
    attrMap = MinorAttrsMap.create(attrs);
    partitionInfoMap = new ConcurrentHashMap<>();
  }

  public static SharedAttrsIndex create(List<MinorAttr> attrList) {
    return new SharedAttrsIndex(attrList);

  }

  public ByteString getAttrValue(OpProps props, String key) {
    return attrMap.getAttrValue(props, key);
  }

  /**
   * Retrieve and deserialize the partition entry.
   */
  public NormalizedPartitionInfo getSplitPartitionEntry(OpProps props, String name) {
    // Cache de-serialized entry to optimise future requests and reduce heap usage.
    Key key = new Key(props.getOperatorId(), name);
    return partitionInfoMap.computeIfAbsent(
      key,
      k -> {
        try {
          return NormalizedPartitionInfo.parseFrom(attrMap.getAttrValue(key));
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("failed to parse partition entry", e);
        }
      });
  }

  public static class Builder {
    private Map<Key, ByteString> attrMap = new HashMap<>();

    public Builder() {}

    public interface Serializer {
      ByteString Serialize();
    }

    public void addAttr(OpProps props, String name, Serializer serializer) {
      Key key = new Key(props.getOperatorId(), name);
      if (!attrMap.containsKey(key)) {
        // delay serialization till we know it's a unique attr.
        attrMap.put(key, serializer.Serialize());
      }
    }

    public List<MinorAttr> getAllAttrs() {
      return attrMap
        .entrySet()
        .stream()
        .map(e -> MinorAttr
          .newBuilder()
          .setOperatorId(e.getKey().getOperatorId())
          .setName(e.getKey().getName())
          .setValue(e.getValue())
          .build()
        )
        .collect(Collectors.toList());
    }
  }
}
