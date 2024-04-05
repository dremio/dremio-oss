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

import com.dremio.io.file.FileBlockLocation;
import com.google.common.collect.ImmutableList;
import java.util.List;

public final class SimpleFileBlockLocation implements FileBlockLocation {
  private final long offset;
  private final long size;
  private final ImmutableList<String> hosts;

  public SimpleFileBlockLocation(long offset, long size, List<String> hosts) {
    this.offset = offset;
    this.size = size;
    this.hosts = ImmutableList.copyOf(hosts);
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
    return hosts;
  }

  @Override
  public String toString() {
    return "SimpleFileBlockLocation [offset="
        + offset
        + ", size="
        + size
        + ", hosts="
        + hosts
        + "]";
  }
}
