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

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

/**
 * Presents an interface that describes the number of bytes for a particular work unit associated with a particular NodeEndpoint.
 */
public interface EndpointByteMap extends Iterable<ObjectLongCursor<NodeEndpoint>>{

  public boolean isSet(NodeEndpoint endpoint);
  public long get(NodeEndpoint endpoint);
  public boolean isEmpty();
  public long getMaxBytes();
  public void add(NodeEndpoint endpoint, long bytes);
}
