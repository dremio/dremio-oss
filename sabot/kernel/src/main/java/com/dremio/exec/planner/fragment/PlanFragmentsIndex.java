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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.exec.proto.CoordExecRPC.MinorAttr;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;

/**
 * Use the index to resolve references to repetitive attributes or objects in the plan.
 */
public class PlanFragmentsIndex {
  private final EndpointsIndex endpointsIndex;
  private final SharedAttrsIndex sharedAttrsIndex;

  public PlanFragmentsIndex(List<NodeEndpoint> endpoints, List<MinorAttr> attrs) {
    endpointsIndex = new EndpointsIndex(endpoints);
    sharedAttrsIndex = SharedAttrsIndex.create(attrs);
  }

  public EndpointsIndex getEndpointsIndex() {
    return endpointsIndex;
  }

  public SharedAttrsIndex getSharedAttrsIndex() {
    return sharedAttrsIndex;
  }

  public static class Builder {
    EndpointsIndex.Builder endpointsIndexBuilder;
    Map<NodeEndpoint, SharedAttrsIndex.Builder> sharedAttrsIndexBuilderMap;

    public Builder() {
      endpointsIndexBuilder = new EndpointsIndex.Builder();
      sharedAttrsIndexBuilderMap = new HashMap<>();
    }

    public EndpointsIndex.Builder getEndpointsIndexBuilder() {
      return endpointsIndexBuilder;
    }

    /**
     * The shared attribute index is built separately for each endpoint.
     */
    public SharedAttrsIndex.Builder getSharedAttrsIndexBuilder(NodeEndpoint endpoint) {
      return sharedAttrsIndexBuilderMap.computeIfAbsent(endpoint,
        k -> new SharedAttrsIndex.Builder());
    }
  }
}
