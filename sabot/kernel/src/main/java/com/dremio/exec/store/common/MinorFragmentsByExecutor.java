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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * A container for executor identity and all minor fragments on it
 */
public class MinorFragmentsByExecutor implements Comparable<MinorFragmentsByExecutor> {
  private final HostAndPort hostPort;
  private final List<Integer> minorFragmentIndices = new ArrayList<>();
  private int currMinorFragIndex;

  public MinorFragmentsByExecutor(HostAndPort hostPort) {
    this.hostPort = hostPort;
  }

  public void addMinorFragmentEndPoint(int index) {
    minorFragmentIndices.add(index);
  }

  public int minorFragmentIndex() {
    Preconditions.checkState(minorFragmentIndices.size() > 0 && currMinorFragIndex < minorFragmentIndices.size());
    int index = minorFragmentIndices.get(currMinorFragIndex);
    currMinorFragIndex = (currMinorFragIndex + 1) % minorFragmentIndices.size();
    return index;
  }

  public String getHostAddress() {
    return hostPort.getHost();
  }

  public int getPort() {
    return hostPort.getPort();
  }

  public HostAndPort getHostPort() {
    return hostPort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MinorFragmentsByExecutor that = (MinorFragmentsByExecutor) o;
    return hostPort.equals(that.hostPort) && minorFragmentIndices.equals(that.minorFragmentIndices);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostPort, minorFragmentIndices);
  }

  @Override
  public int compareTo(MinorFragmentsByExecutor o) {
    int result = hostPort.getHost().compareTo(o.hostPort.getHost());
    if (result == 0 && hostPort.hasPort() && o.hostPort.hasPort()) {
      result = Integer.compare(hostPort.getPort(), o.hostPort.getPort());
    }
    return result;
  }

  @Override
  public String toString() {
    return "MinorFragmentsByExecutor{" +
      "hostPort=" + hostPort +
      ", minorFragmentIndices=" + minorFragmentIndices +
      ", currMinorFragIndex=" + currMinorFragIndex +
      '}';
  }
}
