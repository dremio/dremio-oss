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
package com.dremio.exec.store.dfs;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.io.file.FileBlockLocation;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

public final class InstanceFileBlockLocation implements FileBlockLocation {
  private final long offset;
  private final long size;
  private final ImmutableList<HostAndPort> instances;

  public InstanceFileBlockLocation(long offset, long size, List<HostAndPort> instances) {
    this.offset = offset;
    this.size = size;
    this.instances = ImmutableList.copyOf(instances);
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public List<String> getHosts() {
    return instances.stream().map(HostAndPort::toString).collect(Collectors.toList());
  }

  @Override
  public List<HostAndPort> getHostsWithPorts() {
    return instances;
  }
}
