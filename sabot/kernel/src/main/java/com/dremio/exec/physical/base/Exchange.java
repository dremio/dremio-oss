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
package com.dremio.exec.physical.base;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.planner.fragment.ParallelizationInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Supplier;
import java.util.Collection;
import java.util.List;

public interface Exchange extends PhysicalOperator {

  /**
   * Exchanges are fragment boundaries in physical operator tree. It is divided into two parts.
   * First part is Sender which becomes part of the sending fragment. Second part is Receiver which
   * becomes part of the fragment that receives the data.
   *
   * <p>Assignment dependency describes whether sender fragments depend on receiver fragment's
   * endpoint assignment for determining its parallelization and endpoint assignment and vice versa.
   */
  enum ParallelizationDependency {
    SENDER_DEPENDS_ON_RECEIVER, // Sending fragment depends on receiving fragment for
    // parallelization
    RECEIVER_DEPENDS_ON_SENDER, // Receiving fragment depends on sending fragment for
    // parallelization (default value).
    RECEIVER_MUST_MATCH_SENDER, // Receiving fragment must have the exact same parallelization as
    // sender.
  }

  /**
   * Inform this Exchange node about its sender locations. This list should be index-ordered the
   * same as the expected minorFragmentIds for each sender.
   *
   * @param senderLocations
   */
  void setupSenders(int majorFragmentId, List<NodeEndpoint> senderLocations)
      throws PhysicalOperatorSetupException;

  /**
   * Inform this Exchange node about its receiver locations. This list should be index-ordered the
   * same as the expected minorFragmentIds for each receiver.
   *
   * @param receiverLocations
   */
  void setupReceivers(int majorFragmentId, List<NodeEndpoint> receiverLocations)
      throws PhysicalOperatorSetupException;

  /**
   * Get the Sender associated with the given minorFragmentId. Cannot be called until after
   * setupSenders() and setupReceivers() have been called.
   *
   * @param minorFragmentId The minor fragment id, must be in the range [0, fragment.width).
   * @param child The feeding node for the requested sender.
   * @param builder The index builder.
   * @return The materialized sender for the given arguments.
   */
  Sender getSender(int minorFragmentId, PhysicalOperator child, EndpointsIndex.Builder builder)
      throws PhysicalOperatorSetupException;

  /**
   * Get the Receiver associated with the given minorFragmentId. Cannot be called until after
   * setupSenders() and setupReceivers() have been called.
   *
   * @param minorFragmentId The minor fragment id, must be in the range [0, fragment.width).
   * @param builder The index builder.
   * @return The materialized recevier for the given arguments.
   */
  Receiver getReceiver(int minorFragmentId, EndpointsIndex.Builder builder);

  /** Provide a width constraint for the sender side of the exchange */
  @JsonIgnore
  ParallelizationInfo.WidthConstraint getSenderParallelizationWidthConstraint();

  /**
   * Provide the endpoint affinity for the sender side of the exchange, given the receiver's
   * endpoints (if available)
   *
   * @param receiverFragmentEndpointsSupplier Supplier for the endpoints assigned to the receiver
   *     fragment if available, otherwise an empty list.
   */
  @JsonIgnore
  Supplier<Collection<EndpointAffinity>> getSenderEndpointffinity(
      Supplier<Collection<NodeEndpoint>> receiverFragmentEndpointsSupplier);

  /** Provide a width constraint for the receiver side of the exchange */
  @JsonIgnore
  ParallelizationInfo.WidthConstraint getReceiverParallelizationWidthConstraint();

  /**
   * Provide the endpoint affinity for the receiver side of the exchange
   *
   * @param senderFragmentEndpointsSupplier Supplier for the endpoints assigned to the sender
   *     fragment if available, otherwise an empty list.
   */
  @JsonIgnore
  Supplier<Collection<EndpointAffinity>> getReceiverEndpointAffinity(
      Supplier<Collection<NodeEndpoint>> senderFragmentEndpointsSupplier);

  /**
   * Return the feeding child of this operator node.
   *
   * @return
   */
  PhysicalOperator getChild();

  /** Get the parallelization dependency of the Exchange. */
  @JsonIgnore
  ParallelizationDependency getParallelizationDependency();
}
