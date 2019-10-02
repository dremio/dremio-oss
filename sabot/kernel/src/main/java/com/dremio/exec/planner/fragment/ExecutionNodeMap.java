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
package com.dremio.exec.planner.fragment;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;

/**
 * Holds the mapping of execution nodes available in the cluster.
 */
public class ExecutionNodeMap {

  // We use multimap because more than one node can run in the same cluster.
  private final ListMultimap<String, NodeEndpoint> nodeMap = ArrayListMultimap.create();
  private final List<NodeEndpoint> endpoints;

  public ExecutionNodeMap(Iterable<NodeEndpoint> endpoints){
    this.endpoints = FluentIterable.from(endpoints).toList();
    for(NodeEndpoint ep : endpoints){
      nodeMap.put(ep.getAddress(), ep);
    }
  }

  public NodeEndpoint getExactEndpoint(String hostName, long port) {
    List<NodeEndpoint> endpoints = nodeMap.get(hostName);
    if (endpoints == null || endpoints.isEmpty()) {
      return null;
    }

    for(NodeEndpoint endpoint : endpoints) {
      if (endpoint.getFabricPort() == port) {
        return endpoint;
      }
    }

    return null;
  }

  public NodeEndpoint getEndpoint(String address){
    List<NodeEndpoint> endpoints = nodeMap.get(address);
    if(endpoints == null || endpoints.isEmpty()){
      return null;
    }
    if(endpoints.size() == 1){
      return endpoints.get(0);
    } else {
      // if there is more than one endpoint on the same host, pick a random one.
      return endpoints.get(ThreadLocalRandom.current().nextInt(endpoints.size()));
    }
  }

  public Collection<String> getHosts() {
    return nodeMap.keySet();
  }

  public List<NodeEndpoint> getExecutors(){
    return endpoints;
  }
}
