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
package com.dremio.io.file;

import com.google.common.net.HostAndPort;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A file block location
 *
 * <p>Files are composed of a sequence of blocks. Blocks do partition the whole file and do not
 * overlap. Each non-empty file has a least one block, with at least one host
 */
public interface FileBlockLocation {

  /**
   * Gets the block offset relative to the start of the file
   *
   * @return
   */
  long getOffset();

  /**
   * Gets the size in bytes of the block
   *
   * @return the block size
   */
  long getSize();

  /**
   * Gets the list of hosts physically storing the block.
   *
   * <p>For non-distributed filesystems or filesystems without hosts information available,
   * localhost is used instead.
   *
   * @return a list of hosts. Should contain at least one host
   */
  List<String> getHosts();

  /**
   * Gets the list of hosts with ports physically storing the block.
   *
   * @return a list of hosts with ports. Should contain at least one HostAndPort
   */
  default List<HostAndPort> getHostsWithPorts() {
    return getHosts().stream().map((h) -> HostAndPort.fromHost(h)).collect(Collectors.toList());
  }
}
