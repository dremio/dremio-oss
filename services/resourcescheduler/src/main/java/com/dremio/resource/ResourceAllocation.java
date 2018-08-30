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


import com.dremio.exec.proto.CoordinationProtos;

/**
 * Resource container as part of resource allocation
 * resources released when ResourceAllocation is closed
 */
public interface ResourceAllocation extends Closeable {

  /**
   * Produces NodeEndpoint for the allocated resource
   * @return NodeEndpoint
   */
  CoordinationProtos.NodeEndpoint getEndPoint();

  /**
   * Major Fragment this allocation belongs to
   * @return
   */
  int getMajorFragment();

  /**
   * Allocated memory per resource
   * @return
   */
  long getMemory();

  /**
   * Allocated Resource ID
   * @return id
   */
  long getId();

  /**
   * Assign Major FragmentId
   */
  void assignMajorFragment(int majorFragmentId);
}
