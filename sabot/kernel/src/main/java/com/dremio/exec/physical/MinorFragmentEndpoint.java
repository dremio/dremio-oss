/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.physical;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * MinorFragmentEndpoint represents fragment's MinorFragmentId and SabotNode endpoint to which the fragment is
 * assigned for execution.
 */
@JsonTypeName("fragment-endpoint")
public class MinorFragmentEndpoint {
  private final int id;
  private final NodeEndpoint endpoint;

  @JsonCreator
  public MinorFragmentEndpoint(@JsonProperty("minorFragmentId") int id, @JsonProperty("endpoint") NodeEndpoint endpoint) {
    this.id = id;
    this.endpoint = endpoint;
  }

  /**
   * Get the minor fragment id.
   * @return Minor fragment id.
   */
  @JsonProperty("minorFragmentId")
  public int getId() {
    return id;
  }

  /**
   * Get the SabotNode endpoint where the fragment is assigned for execution.
   *
   * @return SabotNode endpoint.
   */
  @JsonProperty("endpoint")
  public NodeEndpoint getEndpoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    return "FragmentEndPoint: id = " + id + ", ep = " + endpoint;
  }
}
