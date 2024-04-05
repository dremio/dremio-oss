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

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * An extension for Rendezvous hash class that does gives top "X" nodes for a given key.
 *
 * @param <K> type of key
 * @param <N> type node/site or whatever want to be returned (ie IP address or String)
 */
public class MultiValuedRendezvousHash<K, N extends Comparable<? super N>>
    extends RendezvousHash<K, N> {

  public MultiValuedRendezvousHash(
      HashFunction hasher, Funnel<K> keyFunnel, Funnel<N> nodeFunnel, Collection<N> init) {
    super(hasher, keyFunnel, nodeFunnel, init);
  }

  /**
   * Returns top X nodes for a given key.
   *
   * @param key
   * @param numNodes
   * @return
   */
  public List<N> get(K key, int numNodes) {
    PriorityQueue<Long> nodeHashes = new PriorityQueue<>(numNodes);
    Map<Long, N> nodeMap = new HashMap<>();
    for (N node : getOrdered()) {
      long nodeHash =
          getHasher()
              .newHasher()
              .putObject(key, getKeyFunnel())
              .putObject(node, getNodeFunnel())
              .hash()
              .asLong();
      if (canAdd(nodeHash, nodeHashes, numNodes)) {
        nodeMap.put(nodeHash, node);
      }
    }
    return nodeHashes.stream()
        .sorted(Comparator.reverseOrder())
        .map(nh -> nodeMap.get(nh))
        .collect(Collectors.toList());
  }

  private boolean canAdd(long nodeHash, PriorityQueue<Long> nodeHashes, int numNodes) {
    if (nodeHashes.size() < numNodes) {
      nodeHashes.add(nodeHash);
      return true;
    }
    if (nodeHashes.peek() < nodeHash) {
      nodeHashes.poll();
      nodeHashes.add(nodeHash);
      return true;
    }
    return false;
  }
}
