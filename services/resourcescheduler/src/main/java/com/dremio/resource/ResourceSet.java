/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.resource;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.collect.Lists;


/**
 * To return allocated resources
 */
public interface ResourceSet extends Closeable {

  /**
   * Returns list of ResourceAllocation
   * @return List<ResourceAllocation>
   */
  List<ResourceAllocation> getResourceAllocations();

  /**
   * Re/Assign Major Fragment IDs based on final nodes distribution
   * @param majorToEndpoinsMap - distribution of NodeEndPoints per MajorFragment
   */
  void reassignMajorFragments(Map<Integer, Map<NodeEndpoint, Integer>> majorToEndpoinsMap);

  @Override
  /**
   * At this point Resource Allocation will be considered complete
   * all unused resources released, query ends from resource allocation prospective
   */
  void close() throws IOException;

  /**
   * NoOp Implementation if needed to operations that don't deal with resource allocations
   */
  class ResourceSetNoOp implements ResourceSet {

    @Override
    public List<ResourceAllocation> getResourceAllocations() {
      return Lists.newArrayList();
    }

    @Override
    public void reassignMajorFragments(Map<Integer, Map<NodeEndpoint, Integer>> majorToEndpoinsMap) {

    }

    @Override
    public void close() throws IOException {

    }
  }
}


