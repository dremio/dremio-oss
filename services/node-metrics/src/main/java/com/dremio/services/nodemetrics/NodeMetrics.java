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
package com.dremio.services.nodemetrics;

import org.immutables.value.Value;

/** Metrics about a coordinator or executor */
@Value.Immutable
public interface NodeMetrics {

  /** Node name */
  String getName();

  /** Node hostname */
  String getHost();

  /** Node IP address */
  String getIp();

  /** Node user (conduit) port */
  Integer getPort();

  /** Percentage of CPU used over the last second; range: [0, 100] */
  Double getCpu();

  /** Percentage of heap memory (if coordinator) or direct memory (if executor) in use presently */
  Double getMemory();

  /** "green" if node is currently accessible; "red" otherwise */
  String getStatus();

  /** Whether the node is the master coordinator */
  boolean getIsMaster();

  /** Whether the node is a coordinator; note that it may be both an executor and coordinator */
  boolean getIsCoordinator();

  /** Whether the node is an executor; note that it may be both an executor and coordinator */
  boolean getIsExecutor();

  /** Whether the node is running the same version as the cluster version */
  boolean getIsCompatible();

  /** Node tag */
  String getNodeTag();

  /** Node version */
  String getVersion();

  /** Node start time, milliseconds since epoch */
  Long getStart();

  /** String representation of NodeState enum */
  String getDetails();
}
