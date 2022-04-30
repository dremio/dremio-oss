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

package com.dremio.exec.planner.physical.filter;

import java.util.Objects;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.common.MoreRelOptUtil;

public class RuntimeFilterId {
  private final long value;
  private final boolean isBroadcastJoin;

  public RuntimeFilterId(long value, boolean isBroadcastJoin) {
    this.value = value;
    this.isBroadcastJoin = isBroadcastJoin;
  }

  public long getValue() {
    return value;
  }

  public boolean isBroadcastJoin() {
    return this.isBroadcastJoin;
  }

  @Override
  public String toString() {
    //This is used in rel digests
    return Long.toHexString(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RuntimeFilterId that = (RuntimeFilterId) o;
    return value == that.value && isBroadcastJoin == that.isBroadcastJoin;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, isBroadcastJoin);
  }

  public static RuntimeFilterId createRuntimeFilterId(RelNode relNode, boolean isBroadcastJoin) {
    return new RuntimeFilterId(MoreRelOptUtil.longHashCode(relNode), isBroadcastJoin);
  }

}
