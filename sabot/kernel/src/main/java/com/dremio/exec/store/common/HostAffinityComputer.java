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
package com.dremio.exec.store.common;

import com.dremio.io.file.FileBlockLocation;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocations;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility to compute host affinities for a split */
public class HostAffinityComputer {
  // Computes the host affinities for the given split and returns a list sorted in decreasing order
  // of affinities
  public static List<EndpointsAffinity> computeSortedAffinitiesForSplit(
      long splitStart, long splitLength, Iterable<BlockLocations> blockLocations) {
    Map<HostAndPort, Float> hostAffinityMap = new HashMap<>();
    for (BlockLocations blockLocation : blockLocations) {
      long overlapLength =
          getOverlapLength(
              splitStart, splitLength, blockLocation.getOffset(), blockLocation.getSize());
      if (overlapLength <= 0) {
        continue;
      }

      float affinity = (float) overlapLength / splitLength;
      for (String hostPort : blockLocation.getHostsList()) {
        HostAndPort key = HostAndPort.fromString(hostPort);
        hostAffinityMap.merge(key, affinity, Float::sum);
      }
    }

    return sortByDecreasingValues(hostAffinityMap);
  }

  public static Map<HostAndPort, Float> computeAffinitiesForSplit(
      long splitStart,
      long splitLength,
      Iterable<FileBlockLocation> blockLocations,
      boolean preserveBlockLocationsOrder) {
    Map<HostAndPort, Float> hostAffinityMap = Maps.newHashMap();
    for (FileBlockLocation blockLocation : blockLocations) {
      long overlapLength =
          getOverlapLength(
              splitStart, splitLength, blockLocation.getOffset(), blockLocation.getSize());
      float newAffinity = (float) overlapLength / splitLength;
      for (HostAndPort host : blockLocation.getHostsWithPorts()) {
        hostAffinityMap.merge(host, newAffinity, Float::sum);

        /*
         * Preserve the order of the hosts for cloud cache to ensure cache hits.
         * It also guarantees the fragment goes to the next best host in case of failure.
         */
        if (preserveBlockLocationsOrder) {
          newAffinity /= 2;
        }
      }
    }
    return hostAffinityMap;
  }

  private static long getOverlapLength(
      long splitStart, long splitLength, long blockStart, long blockLength) {
    long splitEnd = splitStart + splitLength;
    long blockEnd = blockStart + blockLength;
    return blockLength
        - (blockStart < splitStart ? splitStart - blockStart : 0)
        - (blockEnd > splitEnd ? blockEnd - splitEnd : 0);
  }

  private static List<EndpointsAffinity> sortByDecreasingValues(Map<HostAndPort, Float> map) {
    return map.entrySet().stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .map(entry -> new EndpointsAffinity(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }
}
