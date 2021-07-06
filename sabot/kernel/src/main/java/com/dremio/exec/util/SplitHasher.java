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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.collections.Tuple;
import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.common.EndpointsAffinity;
import com.dremio.exec.store.common.HostAffinityComputer;
import com.dremio.exec.store.common.MinorFragmentsByExecutor;
import com.dremio.exec.util.rhash.MultiValuedRendezvousHash;
import com.dremio.exec.util.rhash.RendezvousPageHasher.PathOffset;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.google.common.net.HostAndPort;

/**
 * Split distributor
 */
public class SplitHasher {
  private static final Logger logger = LoggerFactory.getLogger(SplitHasher.class);

  private final Map<HostAndPort, MinorFragmentsByExecutor> endPoints = new HashMap<>();
  private final MultiValuedRendezvousHash<PathOffset, MinorFragmentsByExecutor> hasher;

  private Map<String, Long> loadPerHost;
  private Map<String, MinorFragmentsByExecutor> endPointEntries;

  public SplitHasher(List<MinorFragmentEndpoint> minorFragmentEndpoints) {
    for (int index = 0; index < minorFragmentEndpoints.size(); index++) {
      MinorFragmentEndpoint minorFragmentEndpoint = minorFragmentEndpoints.get(index);
      CoordinationProtos.NodeEndpoint endpoint = minorFragmentEndpoint.getEndpoint();
      HostAndPort nodeIdentity = HostAndPort.fromParts(endpoint.getAddress(), endpoint.getFabricPort());
      MinorFragmentsByExecutor node = endPoints.get(nodeIdentity);
      if (node == null) {
        node = new MinorFragmentsByExecutor(nodeIdentity);
        endPoints.put(nodeIdentity, node);
      }
      node.addMinorFragmentEndPoint(index);
    }

    logger.debug("NodeEndpoints: {}", endPoints.values());

    this.hasher = new MultiValuedRendezvousHash<>(
      Hashing.murmur3_128(),
      (k, f) -> f.putString(k.getPath(), StandardCharsets.UTF_8).putLong(k.getOffset()),
      (n, f) -> f.putString(n.getHostAddress(), StandardCharsets.UTF_8).putInt(n.getPort()),
      endPoints.values());
  }

  public int getMinorFragmentIndex(Path path, BlockLocationsList blockLocationsList, long splitOffset, long splitLength) {
    logger.debug("Path: {}, blockLocationsList: {}, splitOffset: {}, splitLength: {}",
      path, blockLocationsList, splitOffset, splitLength);
    if (blockLocationsList != null && blockLocationsList.getBlockLocationsCount() != 0) {
      List<EndpointsAffinity> endpointAffinities = HostAffinityComputer.computeSortedAffinitiesForSplit(
        splitOffset, splitLength, blockLocationsList.getBlockLocationsList());
      logger.debug("endpointAffinities: {}", endpointAffinities);

      if (!endpointAffinities.isEmpty()) {
        initMaps(endpointAffinities.get(0).isInstanceAffinity());
        Integer minorFragment = findHostWithHighestAffinity(endpointAffinities, splitLength);
        logger.debug("Target minor fragment: {}", minorFragment);
        if (minorFragment != null) {
          return minorFragment;
        }
      }
    }

    logger.debug("No block locations or overlap found. Falling back to rendezvous hashing");
    return getMinorFragmentIndexUsingRendezvousHash(path, splitOffset);
  }

  @VisibleForTesting
  public int getMinorFragmentIndexUsingRendezvousHash(Path path, long offset) {
    PathOffset pathOffset = new PathOffset(path.toString(), offset);
    int minorFragment = hasher.get(pathOffset, 1).get(0).minorFragmentIndex();
    logger.debug("Path: {}, Target minor fragment: {}", pathOffset.getPath(), minorFragment);
    return minorFragment;
  }

  private Integer findHostWithHighestAffinity(List<EndpointsAffinity> endpointsAffinities, long length) {
    List<EndpointsAffinity> validEndPointAffinities = filterAndAugment(endpointsAffinities);
    logger.debug("Valid endpointsAffinities: {}", validEndPointAffinities);
    if (validEndPointAffinities.isEmpty()) {
      return null;
    }

    EndpointsAffinity endpointsAffinity = validEndPointAffinities.get(0);
    float affinity = endpointsAffinity.getAffinity();
    String selectedHostPort = leastLoadedHighestAffinityHost(validEndPointAffinities, affinity);
    loadPerHost.merge(selectedHostPort, length, Long::sum);
    return endPointEntries.get(selectedHostPort).minorFragmentIndex();
  }

  private String leastLoadedHighestAffinityHost(List<EndpointsAffinity> validEndPointAffinities, float affinity) {
    EndpointsAffinity endpointsAffinity = validEndPointAffinities.get(0);
    String hostPort = endpointsAffinity.getHostAndPort().toString();
    Tuple<String, Long> highestAffinityMinLoadedHost = Tuple.of(hostPort, loadPerHost.get(hostPort));
    for (int index = 1; index < validEndPointAffinities.size(); index++) {
      if (validEndPointAffinities.get(index).getAffinity() < affinity) {
        break;
      }

      String currHostPort = validEndPointAffinities.get(index).getHostAndPort().toString();
      Long currLoad = loadPerHost.get(currHostPort);
      if (currLoad < highestAffinityMinLoadedHost.second) {
        highestAffinityMinLoadedHost = Tuple.of(currHostPort, currLoad);
      }
    }
    return highestAffinityMinLoadedHost.first;
  }

  // filters non-executor nodes and set minor fragments info
  private List<EndpointsAffinity> filterAndAugment(List<EndpointsAffinity> endpointsAffinities) {
    List<EndpointsAffinity> list = new ArrayList<>();
    for (EndpointsAffinity ea : endpointsAffinities) {
      MinorFragmentsByExecutor minorFragmentsByExecutor = endPointEntries.get(ea.getHostAndPort().toString());
      if (minorFragmentsByExecutor != null) {
        EndpointsAffinity endpointsAffinity = ea.setMinorFragmentsByExecutor(minorFragmentsByExecutor);
        list.add(endpointsAffinity);
      }
    }
    return list;
  }

  private void initMaps(boolean isInstanceAffinity) {
    if (endPointEntries == null) {
      endPointEntries = new HashMap<>();
      for (MinorFragmentsByExecutor minorFragmentsByExecutor : endPoints.values()) {
        if (isInstanceAffinity) {
          endPointEntries.put(minorFragmentsByExecutor.getHostPort().toString(), minorFragmentsByExecutor);
        } else {
          endPointEntries.put(minorFragmentsByExecutor.getHostPort().getHost(), minorFragmentsByExecutor);
        }
      }
    }

    if (loadPerHost == null) {
      loadPerHost = new HashMap<>();
      for (MinorFragmentsByExecutor minorFragmentsByExecutor : endPoints.values()) {
        if (isInstanceAffinity) {
          loadPerHost.put(minorFragmentsByExecutor.getHostPort().toString(), 0L);
        } else {
          loadPerHost.put(minorFragmentsByExecutor.getHostPort().getHost(), 0L);
        }
      }
    }
  }
}
