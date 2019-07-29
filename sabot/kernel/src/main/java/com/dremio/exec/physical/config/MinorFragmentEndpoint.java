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
package com.dremio.exec.physical.config;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.protobuf.TextFormat;

/**
 * A minor fragment endpoint, including the minor fragment id and the endpoint.
 */
public class MinorFragmentEndpoint {
  private final int minorFragmentId;
  private final NodeEndpoint endpoint;

  public MinorFragmentEndpoint(int minorFragmentId, NodeEndpoint endpoint) {
    this.minorFragmentId = minorFragmentId;
    this.endpoint = endpoint;
  }

  public int getMinorFragmentId() {
    return minorFragmentId;
  }
  public NodeEndpoint getEndpoint() {
    return endpoint;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof MinorFragmentEndpoint)) {
      return false;
    }

    MinorFragmentEndpoint other = (MinorFragmentEndpoint) obj;
    if (endpoint == null) {
      if (other.endpoint != null) {
        return false;
      }
    } else if (!endpoint.equals(other.endpoint)) {
      return false;
    }
    return minorFragmentId == other.getMinorFragmentId();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp = minorFragmentId;
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "MinorFragmentEndpoint " +
      " [endpoint=" + TextFormat.shortDebugString(endpoint) +
      ", minorFragmentId=" + minorFragmentId +
      "]";
  }
}
