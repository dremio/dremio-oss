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
package com.dremio.exec.store.schedule;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

public class PartialWork {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartialWork.class);

  private final long length;
  private final NodeEndpoint[] locations;

  public PartialWork(long length, NodeEndpoint[] locations) {
    super();
    this.length = length;
    this.locations = locations;
  }

  public long getLength() {
    return length;
  }
  public NodeEndpoint[] getLocations() {
    return locations;
  }



}
