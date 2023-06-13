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

import java.util.Iterator;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.google.common.net.HostAndPort;

public class EndpointByteMapImpl implements EndpointByteMap{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointByteMapImpl.class);

  private final ObjectLongHashMap<HostAndPort> map = new ObjectLongHashMap<>();

  private long maxBytes;

  @Override
  public boolean isSet(HostAndPort endpoint){
    return map.containsKey(endpoint);
  }

  @Override
  public long get(HostAndPort endpoint){
    return map.get(endpoint);
  }

  @Override
  public boolean isEmpty(){
    return map.isEmpty();
  }

  @Override
  public void add(HostAndPort endpoint, long bytes){
    assert endpoint != null;
    maxBytes = Math.max(maxBytes, map.putOrAdd(endpoint, bytes, bytes)+1);
  }

  @Override
  public long getMaxBytes() {
    return maxBytes;
  }

  @Override
  public Iterator<ObjectLongCursor<HostAndPort>> iterator() {
    return map.iterator();
  }


}
