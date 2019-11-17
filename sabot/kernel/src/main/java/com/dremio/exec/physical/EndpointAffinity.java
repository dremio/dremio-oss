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
package com.dremio.exec.physical;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

/**
 * EndpointAffinity captures affinity value for a given single SabotNode endpoint.
 */
public class EndpointAffinity {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EndpointAffinity.class);

  private final NodeEndpoint endpoint;
  private final HostAndPort hostAndPort;
  private double affinity = 0.0d;

  // Requires including this endpoint at least once? Default is not required.
  private boolean mandatory;

  /**
   * Maximum allowed assignments for this endpoint. Default is {@link Integer#MAX_VALUE}
   */
  private int maxWidth = Integer.MAX_VALUE;

  public static EndpointAffinity fromHostAndPort(HostAndPort hostAndPort, double affinity) {
    return new EndpointAffinity(hostAndPort, getNodeEndpointFromHostPort(hostAndPort), affinity, false, Integer.MAX_VALUE);
  }

  /**
   * Create EndpointAffinity instance for given SabotNode endpoint. Affinity is initialized to 0. Affinity can be added
   * after EndpointAffinity object creation using {@link #addAffinity(double)}.
   *
   * @param endpoint Sabot endpoint
   */
  public EndpointAffinity(NodeEndpoint endpoint) {
    this.endpoint = endpoint;
    this.hostAndPort = getHostPortFromNodeEndpoint(endpoint);
  }

  /**
   * Create EndpointAffinity instance for given SabotNode endpoint and affinity initialized to given affinity value.
   * Affinity can be added after EndpointAffinity object creation using {@link #addAffinity(double)}.
   *
   * @param endpoint Sabot endpoint
   * @param affinity Initial affinity value.
   */
  public EndpointAffinity(NodeEndpoint endpoint, double affinity) {
    this.endpoint = endpoint;
    this.hostAndPort = getHostPortFromNodeEndpoint(endpoint);
    this.affinity = affinity;
  }

  /**
   * Creates EndpointAffinity instance for given NodeEndpoint, affinity and mandatory assignment requirement flag.
   * @param endpoint Sabot endpoint
   * @param affinity Initial affinity value
   * @param mandatory Is this endpoint requires at least one mandatory assignment?
   * @param maxWidth Maximum allowed assignments for this endpoint.
   */
  public EndpointAffinity(NodeEndpoint endpoint, final double affinity, final boolean mandatory, final int maxWidth) {
    this(getHostPortFromNodeEndpoint(endpoint), endpoint, affinity, mandatory, maxWidth);
  }

  /**
   * Creates EndpointAffinity instance for given NodeEndpoint, affinity and mandatory assignment requirement flag.
   * @param hostAndPort host and port
   * @param affinity Initial affinity value
   * @param mandatory Is this endpoint requires at least one mandatory assignment?
   * @param maxWidth Maximum allowed assignments for this endpoint.
   */
  public EndpointAffinity(HostAndPort hostAndPort, final double affinity, final boolean mandatory, final int maxWidth) {
    this(hostAndPort, getNodeEndpointFromHostPort(hostAndPort), affinity, mandatory, maxWidth);
  }

  /**
   * Creates EndpointAffinity instance for given NodeEndpoint, affinity and mandatory assignment requirement flag.
   * @param hostAndPort host and port
   * @param endpoint Sabot endpoint
   * @param affinity Initial affinity value
   * @param mandatory Is this endpoint requires at least one mandatory assignment?
   * @param maxWidth Maximum allowed assignments for this endpoint.
   */
  public EndpointAffinity(final HostAndPort hostAndPort, final NodeEndpoint endpoint, final double affinity, final boolean mandatory, final int maxWidth) {
    Preconditions.checkArgument(maxWidth >= 1, "MaxWidth for given endpoint should be at least one.");
    this.hostAndPort = hostAndPort;
    this.endpoint = endpoint;
    this.affinity = affinity;
    this.mandatory = mandatory;
    this.maxWidth = maxWidth;
  }

  static NodeEndpoint getNodeEndpointFromHostPort(HostAndPort hostAndPort) {
    NodeEndpoint.Builder builder = NodeEndpoint.newBuilder().setAddress(hostAndPort.getHost());
    if (hostAndPort.hasPort()) {
      builder.setFabricPort(hostAndPort.getPort());
    }

    return builder.build();
  }

  static HostAndPort getHostPortFromNodeEndpoint(NodeEndpoint endpoint) {
    // by default, no instance affinity
    return HostAndPort.fromHost(endpoint.getAddress());
  }

  /**
   * Return the SabotNode endpoint in this instance.
   *
   * @return SabotNode endpoint.
   */
  public NodeEndpoint getEndpoint() {
    return endpoint;
  }

  /**
   * Get the affinity value. Affinity value is Double.POSITIVE_INFINITY if the SabotNode endpoint requires an assignment.
   *
   * @return affinity value
   */
  public double getAffinity() {
    return affinity;
  }

  /**
   * Get isInstanceAffinity value.
   *
   * @return isInstanceAffinity value
   */
  public boolean isInstanceAffinity() {
    return hostAndPort.hasPort();
  }

  /**
   * Add given affinity value to existing affinity value.
   *
   * @param f Affinity value (must be a non-negative value).
   * @throws java.lang.IllegalArgumentException If the given affinity value is negative.
   */
  public void addAffinity(double f){
    Preconditions.checkArgument(f >= 0.0d, "Affinity should not be negative");
    if (Double.POSITIVE_INFINITY == f) {
      affinity = f;
    } else if (Double.POSITIVE_INFINITY != affinity) {
      affinity += f;
    }
  }

  /**
   * Set the endpoint requires at least one assignment.
   */
  public void setAssignmentRequired() {
    mandatory = true;
  }

  /**
   * Is this endpoint required to be in fragment endpoint assignment list?
   *
   * @return Returns true for mandatory assignment, false otherwise.
   */
  public boolean isAssignmentRequired() {
    return mandatory || Double.POSITIVE_INFINITY == affinity;
  }

  /**
   * @return Maximum allowed assignments for this endpoint.
   */
  public int getMaxWidth() {
    return maxWidth;
  }

  /**
   * Set the new max width as the minimum of the the given value and current max width.
   * @param maxWidth
   */
  public void setMaxWidth(final int maxWidth) {
    Preconditions.checkArgument(maxWidth >= 1, "MaxWidth for given endpoint should be at least one.");
    this.maxWidth = Math.min(this.maxWidth, maxWidth);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long temp;
    temp = Double.doubleToLongBits(affinity);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + ((endpoint == null) ? 0 : endpoint.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof EndpointAffinity)) {
      return false;
    }
    EndpointAffinity other = (EndpointAffinity) obj;
    if (Double.doubleToLongBits(affinity) != Double.doubleToLongBits(other.affinity)) {
      return false;
    }
    if (endpoint == null) {
      if (other.endpoint != null) {
        return false;
      }
    } else if (!endpoint.equals(other.endpoint)) {
      return false;
    }
    return mandatory == other.mandatory;
  }

  @Override
  public String toString() {
    return "EndpointAffinity [hostPort=" + hostAndPort.toString() + ", affinity=" + affinity +
        ", mandatory=" + mandatory + ", maxWidth=" + maxWidth + "]";
  }
}
