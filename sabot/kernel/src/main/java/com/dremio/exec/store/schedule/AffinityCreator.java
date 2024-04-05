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

import com.carrotsearch.hppc.ObjectDoubleHashMap;
import com.carrotsearch.hppc.cursors.ObjectDoubleCursor;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AffinityCreator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AffinityCreator.class);

  public static <T extends CompleteWork> List<EndpointAffinity> getAffinityMap(List<T> work) {
    Stopwatch watch = Stopwatch.createStarted();

    double totalBytes = 0;
    for (CompleteWork entry : work) {
      totalBytes += entry.getTotalBytes();
    }

    ObjectDoubleHashMap<NodeEndpoint> affinities = new ObjectDoubleHashMap<NodeEndpoint>();
    for (CompleteWork entry : work) {
      for (EndpointAffinity affinity : entry.getAffinity()) {
        final double bytes = affinity.getAffinity();
        double newAffinityValue = bytes / totalBytes;
        NodeEndpoint endpoint = affinity.getEndpoint();
        affinities.putOrAdd(endpoint, newAffinityValue, newAffinityValue);
      }
    }

    List<EndpointAffinity> affinityList = Lists.newLinkedList();
    for (ObjectDoubleCursor<NodeEndpoint> d : affinities) {
      affinityList.add(new EndpointAffinity(d.key, d.value));
    }

    if (logger.isDebugEnabled()) {
      for (ObjectDoubleCursor<NodeEndpoint> d : affinities) {
        logger.debug("Endpoint {} has affinity {}", d.key.getAddress(), d.value);
      }

      logger.debug("Took {} ms to get operator affinity", watch.elapsed(TimeUnit.MILLISECONDS));
    }

    return affinityList;
  }
}
