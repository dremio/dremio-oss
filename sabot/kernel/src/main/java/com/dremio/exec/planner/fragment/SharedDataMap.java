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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordExecRPC.SharedData;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

/**
 * Holds the shared data for a major fragment. Used to inject into the pops
 */
public class SharedDataMap {
  private final Map<Key, SharedData> map;

  private SharedDataMap(final Map<Key, SharedData> map) {
    this.map = map;
  }

  public byte[] getSharedDataValue(PhysicalOperator pop, String name) {
    SharedData sd = map.get(new Key(pop.getOperatorId(), name));
    Preconditions.checkNotNull(sd, String.format("Could not find shared data value for operator:%d, %s\nMap:%s", pop.getOperatorId(), name, map));
    return sd.getValue().toByteArray();
  }

  public static SharedDataMap create(List<SharedData> sharedDataList, FragmentHandle fragmentHandle) {
    Map<Key,SharedData> map = FluentIterable.from(sharedDataList)
      .filter(s -> s.getMajorFragmentId() == fragmentHandle.getMajorFragmentId())
      .uniqueIndex(s -> new Key(s.getOperatorId(), s.getName()));

    return new SharedDataMap(map);
  }

  private static class Key {
    private final int operatorId;
    private final String name;

    public Key(int operatorId, String name) {
      this.operatorId = operatorId;
      this.name = name;
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
      return operatorId == key.operatorId &&
        Objects.equals(name, key.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operatorId, name);
    }

    @Override
    public String toString() {
      return "Key{" +
        "operatorId=" + operatorId +
        ", name='" + name + '\'' +
        '}';
    }
  }
}
