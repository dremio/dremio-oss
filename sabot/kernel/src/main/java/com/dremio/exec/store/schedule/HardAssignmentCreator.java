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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.FormatMethod;

/**
 * Hard affinity assignments for given endpoints and work units.
 */
public class HardAssignmentCreator<T extends CompleteWork> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HardAssignmentCreator.class);

  public static HardAssignmentCreator INSTANCE = new HardAssignmentCreator();

  public <T extends CompleteWork> ListMultimap<Integer, T> getMappings(
      final List<NodeEndpoint> endpoints, final List<T> units) throws PhysicalOperatorSetupException {
    verify(endpoints, units, units.size() >= endpoints.size(), "There should be at least one work unit for each hard affinity node.");

    // First, group endpoints by hostname. There could be multiple endpoints on same host
    final ListMultimap<String, Integer> endpointsOnHostMap = ArrayListMultimap.create();
    int index = 0;
    for(NodeEndpoint incoming : endpoints) {
      endpointsOnHostMap.put(incoming.getAddress(), index);
      index++;
    }

    // Convert the multi-map <String,Integer> into a map <String, Iterator<Integer>>
    final Map<String, Iterator<Integer>> endpointIteratorOnHostMap = Maps.newHashMap();
    for(Map.Entry<String, Collection<Integer>> entry: endpointsOnHostMap.asMap().entrySet()) {
      endpointIteratorOnHostMap.put(entry.getKey(), Iterables.cycle(entry.getValue()).iterator());
    }

    final ListMultimap<Integer, T> mappings = ArrayListMultimap.create();
    for(T unit: units) {
      final List<EndpointAffinity> affinities = unit.getAffinity();
      verify(endpoints, units, affinities.size() == 1,
          "Expected the hard affinity work unit to have affinity to only one endpoint");
      final EndpointAffinity endpointAffinity = affinities.get(0);
      final String host = endpointAffinity.getEndpoint().getAddress();
      final Iterator<Integer> endpointsOnHost = endpointIteratorOnHostMap.get(host);
      if (endpointsOnHost == null) {
        verify(endpoints, units, false, "There are no endpoints in assigned list running on host %s", host);
      }

      final int endpointId = endpointIteratorOnHostMap.get(host).next();
      mappings.put(endpointId, unit);
    }

    // Go through the mappings and make sure at least one unit for every assigned endpoint,
    // otherwise we will end up with fragments that don't have any work assigned. If an assignment is not present for
    // endpoint, throw an exception
    for(int i = 0; i < endpoints.size(); i++) {
      if (!mappings.containsKey(i)) {
        verify(endpoints, units, false, "Endpoint %s has no assigned work.", endpoints.get(i));
      }
    }

    return mappings;
  }

  @FormatMethod
  private static <T extends CompleteWork> void verify(final List<NodeEndpoint> endpoints, final List<T> units,
      final boolean condition, final String msg, Object... args) {
    if (!condition) {
      throw UserException.resourceError()
          .message(msg, args)
          .addContext("endpoints %s", endpoints)
          .addContext("workunits %s", units)
          .build(logger);
    }
  }
}
