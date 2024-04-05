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
import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Map for minor-specific attributes. */
public class MinorAttrsMap {
  private final Map<Key, MinorAttr> map;

  private MinorAttrsMap(final Map<Key, MinorAttr> map) {
    this.map = map;
  }

  public ByteString getAttrValue(OpProps props, String name) {
    return getAttrValue(new Key(props.getOperatorId(), name));
  }

  public ByteString getAttrValue(Key key) {
    MinorAttr attr = map.get(key);
    if (attr == null) {
      throw new NullPointerException(
          String.format(
              "Could not find minor specific attr value for operator %d-xx-%d, %s\nMap:%s",
              OpProps.getMajorFragmentId(key.getOperatorId()),
              OpProps.getLocalOperatorId(key.getOperatorId()),
              key.getName(),
              map));
    }
    return attr.getValue();
  }

  public static MinorAttrsMap create(List<MinorAttr> attrsList) {
    /* Make it immutable so that it's thread-safe */
    Map<Key, MinorAttr> map =
        ImmutableMap.copyOf(
            attrsList.stream()
                .collect(Collectors.toMap(s -> new Key(s.getOperatorId(), s.getName()), s -> s)));
    return new MinorAttrsMap(map);
  }

  public static class Key {
    private final int operatorId;
    private final String name;

    public Key(int operatorId, String name) {
      this.operatorId = operatorId;
      this.name = name;
    }

    public int getOperatorId() {
      return operatorId;
    }

    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Key key = (Key) o;
      return operatorId == key.operatorId && Objects.equals(name, key.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operatorId, name);
    }

    @Override
    public String toString() {
      return "Key{" + "operatorId=" + operatorId + ", name='" + name + '\'' + '}';
    }
  }
}
