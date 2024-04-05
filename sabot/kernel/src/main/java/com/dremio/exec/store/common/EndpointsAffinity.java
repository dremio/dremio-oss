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
package com.dremio.exec.store.common;

import com.google.common.net.HostAndPort;
import java.util.Objects;

/** A container for executor identity and {@link MinorFragmentsByExecutor} */
public class EndpointsAffinity {
  private final float affinity;
  private final HostAndPort hostAndPort;
  private MinorFragmentsByExecutor minorFragmentsByExecutor;

  public EndpointsAffinity(HostAndPort hostAndPort, float affinity) {
    this.hostAndPort = hostAndPort;
    this.affinity = affinity;
  }

  public MinorFragmentsByExecutor getMinorFragmentsByExecutor() {
    return minorFragmentsByExecutor;
  }

  public EndpointsAffinity setMinorFragmentsByExecutor(
      MinorFragmentsByExecutor minorFragmentsByExecutor) {
    this.minorFragmentsByExecutor = minorFragmentsByExecutor;
    return this;
  }

  public HostAndPort getHostAndPort() {
    return hostAndPort;
  }

  public float getAffinity() {
    return affinity;
  }

  public boolean isInstanceAffinity() {
    return hostAndPort.hasPort();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EndpointsAffinity that = (EndpointsAffinity) o;
    return Float.compare(that.affinity, affinity) == 0
        && minorFragmentsByExecutor.equals(that.minorFragmentsByExecutor)
        && hostAndPort.equals(that.hostAndPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minorFragmentsByExecutor, hostAndPort, affinity);
  }

  @Override
  public String toString() {
    return "EndpointsAffinity [hostPort=" + hostAndPort + ", affinity=" + affinity + "]";
  }
}
