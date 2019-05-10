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
package com.dremio.exec.physical.base;

import java.util.List;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.MinorFragmentIndexEndpoint;
import com.dremio.exec.record.BatchSchema;

/**
 * A sender is one half of an exchange node operations. It is responsible for subdividing/cloning and sending a local
 * record set to a set of destination locations. This is typically only utilized at the level of the execution plan.
 */
public interface Sender extends FragmentRoot {

  /**
   * Get the list of destination endpoints that this Sender will be communicating with.
   * @return List of receiver MinorFragmentIndexEndpoints each containing receiver fragment MinorFragmentId
   * and endpoint index where it is running.
   */
  public abstract List<MinorFragmentIndexEndpoint> getDestinations();

  /**
   * Get the list of destination endpoints that this Sender will be communicating with.
   * @param endpointsIndex Index of endpoints.
   * @return List of receiver MinorFragmentEndpoints each containing receiver fragment MinorFragmentId
   * and endpoint where it is running.
   */
  public abstract List<MinorFragmentEndpoint> getDestinations(EndpointsIndex endpointsIndex);

  /**
   * Get the receiver major fragment id that is opposite this sender.
   * @return
   */
  public int getReceiverMajorFragmentId();

  public PhysicalOperator getChild();

  BatchSchema getSchema();
}
