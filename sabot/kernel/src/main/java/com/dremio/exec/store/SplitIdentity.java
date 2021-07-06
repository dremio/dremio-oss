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
package com.dremio.exec.store;

import java.io.Serializable;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;

/**
 * Split Identity.
 */
public class SplitIdentity implements Serializable {

  private final String path;
  private final PartitionProtobuf.BlockLocationsList blockLocations;
  private final long offset;
  private final long length;

  public SplitIdentity(String path, PartitionProtobuf.BlockLocationsList blockLocations, long offset, long length)
  {
    this.path = path;
    this.blockLocations = blockLocations;
    this.offset = offset;
    this.length = length;
  }

  public String getPath() {
    return path;
  }

  public PartitionProtobuf.BlockLocationsList getBlockLocations() {
    return blockLocations;
  }

  public long getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }
}
