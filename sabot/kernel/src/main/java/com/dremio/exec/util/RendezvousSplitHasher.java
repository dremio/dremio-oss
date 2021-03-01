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
package com.dremio.exec.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.util.rhash.MultiValuedRendezvousHash;
import com.dremio.exec.util.rhash.RendezvousPageHasher;
import com.dremio.io.file.Path;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;

/**
 * Rendezvous hash for split distribution
 */
public class RendezvousSplitHasher {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RendezvousSplitHasher.class);
    private final MultiValuedRendezvousHash<RendezvousPageHasher.PathOffset, NodeEndPoints> hasher;

    /**
     * Host address and port number uniquely identifies a node
     */
    private static class NodeIdentity implements Comparable<NodeIdentity> {
        private final String hostAddress;
        private final int fabricPort;
        public NodeIdentity(String hostAddress, int fabricPort) {
            this.hostAddress = hostAddress;
            this.fabricPort = fabricPort;
        }
        @Override
        public int compareTo(RendezvousSplitHasher.NodeIdentity o) {
            int cmp = this.hostAddress.compareTo(
                    o.hostAddress);
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(this.fabricPort, o.fabricPort);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || this.getClass() != o.getClass()) {
                return false;
            }
            NodeIdentity other = (NodeIdentity) o;
            return hostAddress.equalsIgnoreCase(other.hostAddress) &&
                    fabricPort == other.fabricPort;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hostAddress, fabricPort);
        }
    }

    /**
     * A node has identity and list of minor fragments on it
     */
    private static class NodeEndPoints implements Comparable<NodeEndPoints>{
        private final NodeIdentity nodeIdentity;
        private final ArrayList<Integer> minorFragmentIndices = new ArrayList<>();
        private int currentIndex;

        public NodeEndPoints(NodeIdentity nodeIdentity) {
            this.nodeIdentity = nodeIdentity;
            currentIndex = 0;
        }

        public void addMinorFragmentEndPoint(int index) {
            minorFragmentIndices.add(index);
        }

        public int getNodeIndex() {
            Preconditions.checkState(minorFragmentIndices.size() > 0 && currentIndex < minorFragmentIndices.size());
            int nodeIndex = minorFragmentIndices.get(currentIndex);
            currentIndex = (currentIndex + 1) % minorFragmentIndices.size();
            return nodeIndex;
        }

        public String getHostAddress() {
            return nodeIdentity.hostAddress;
        }

        public int getFabricPort() {
            return nodeIdentity.fabricPort;
        }

        @Override
        public int compareTo(NodeEndPoints o) {
            return this.nodeIdentity.compareTo(o.nodeIdentity);
        }
    }

    public RendezvousSplitHasher(List<MinorFragmentEndpoint> minorFragmentEndpoints) {
        Map<NodeIdentity, NodeEndPoints> endPointEntries = new HashMap<>();
        for (int index = 0; index < minorFragmentEndpoints.size(); index++) {
            MinorFragmentEndpoint minorFragmentEndpoint = minorFragmentEndpoints.get(index);
            NodeIdentity nodeIdentity = new NodeIdentity(minorFragmentEndpoint.getEndpoint().getAddress(),
                                                minorFragmentEndpoint.getEndpoint().getFabricPort());
            NodeEndPoints node = endPointEntries.get(nodeIdentity);
            if (node == null) {
                node = new NodeEndPoints(nodeIdentity);
                endPointEntries.put(nodeIdentity, node);
            }
            node.addMinorFragmentEndPoint(index);
        }

        this.hasher = new MultiValuedRendezvousHash<RendezvousPageHasher.PathOffset, NodeEndPoints>(
                Hashing.murmur3_128(),
                (k,f) -> f.putString(k.getPath(), StandardCharsets.UTF_8).putLong(k.getOffset()),
                (n,f) -> f.putString(n.getHostAddress(), StandardCharsets.UTF_8)
                            .putInt(n.getFabricPort()),
                endPointEntries.values());
    }

    public int getNodeIndex(Path path, long offset) {
        RendezvousPageHasher.PathOffset po = new RendezvousPageHasher.PathOffset(path.toString(), offset);
        int nodeIndex = hasher.get(po, 1).get(0).getNodeIndex();
        logger.trace(String.format("Path: %s, Target node index: %d", path.toString(), nodeIndex));
        return nodeIndex;
    }

}
