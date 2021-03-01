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
package com.dremio.exec.util.rhash;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.io.file.Path;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.hash.Hashing;

/**
 * Pick a set node for a path based on the collection of known nodes.
 */
public class RendezvousPageHasher {

  private static final int NUM_NODES = 3;
  private final MultiValuedRendezvousHash<PathOffset, NodeEntry> hasher;

  public RendezvousPageHasher(ServiceSet serviceSet) {
    this.hasher = new MultiValuedRendezvousHash<PathOffset, NodeEntry>(
            Hashing.murmur3_128(),
        (k,f) -> f.putString(k.getPath(), StandardCharsets.UTF_8).putLong(k.getOffset()),
        (n,f) -> f.putString(n.host, StandardCharsets.UTF_8).putInt(n.fabricPort),
        serviceSet.getAvailableEndpoints()
          .stream()
          .map(n -> new NodeEntry(n))
          .collect(Collectors.toList())
        );

    // listen to node coming and going.
    serviceSet.addNodeStatusListener(new Listener());
  }

  public static final class PathOffset {
    private final String path;
    private final long offset;

    public PathOffset(String path, long offset) {
      super();
      this.path = path;
      this.offset = offset;
    }

    public String getPath() {
      return path;
    }

    public long getOffset() {
      return offset;
    }
  }

  public NodeEndpoint[] getEndpoints(Path path, long offset) {
    PathOffset po = new PathOffset(path.toString(), offset);
    return hasher.get(po, NUM_NODES).stream().map(ne -> ne.endpoint).toArray(NodeEndpoint[]::new);
  }

  private static final class NodeEntry implements Comparable<NodeEntry> {
    private final String host;
    private final int fabricPort;
    private final long createTime;
    private final NodeEndpoint endpoint;

    public NodeEntry(NodeEndpoint endpoint) {
      this.endpoint = endpoint;
      this.host = endpoint.getAddress();
      this.fabricPort = endpoint.getFabricPort();
      this.createTime = endpoint.getStartTime();
    }

    @Override
    public int compareTo(NodeEntry o) {
      int cmp = 0;
      cmp = Long.compare(this.createTime, o.createTime);
      if(cmp != 0) {
        return cmp;
      }
      cmp = host.compareTo(o.host);
      if(cmp != 0) {
        return cmp;
      }
      return Integer.compare(fabricPort, o.fabricPort);
    }

    @Override
    public String toString() {
      return this.host;
    }
  }

  private final class Listener implements NodeStatusListener {

    @Override
    public void nodesRegistered(Set<NodeEndpoint> nodes) {
      nodes.stream().map(NodeEntry::new).forEach(n -> hasher.add(n));
    }

    @Override
    public void nodesUnregistered(Set<NodeEndpoint> nodes) {
      nodes.stream().map(NodeEntry::new).forEach(n -> hasher.remove(n));
    }

  }
}
