/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.schedule;

import java.util.List;

import com.dremio.exec.physical.EndpointAffinity;
import com.google.common.collect.ImmutableList;

public class SimpleCompleteWork implements CompleteWork {

  private final long size;
  private final List<EndpointAffinity> affinity;

  public SimpleCompleteWork(long size, List<EndpointAffinity> affinity) {
    this.size = size;
    this.affinity = affinity;
  }

  public SimpleCompleteWork(long size, EndpointAffinity... affinity) {
    this.size = size;
    this.affinity = ImmutableList.copyOf(affinity);
  }

  public long getSize() {
    return size;
  }

  public List<EndpointAffinity> getAffinity() {
    return affinity;
  }

  @Override
  public int compareTo(CompleteWork o) {
    return Long.compare(getSize(), o.getTotalBytes());
  }

  @Override
  public long getTotalBytes() {
    return size;
  }

}
