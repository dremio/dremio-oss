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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.planner.fragment.ParallelizationInfo;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public abstract class AbstractExchange extends AbstractSingle implements Exchange {

  protected final OpProps senderProps;
  protected final OpProps receiverProps;
  protected final BatchSchema schema;

  // Ephemeral info for generating execution fragments.
  protected int senderMajorFragmentId;
  protected int receiverMajorFragmentId;
  protected List<NodeEndpoint> senderLocations;
  protected List<NodeEndpoint> receiverLocations;

  private boolean isMemReserveUpdated = false;

  private final OptionManager optionManager;

  public AbstractExchange(OpProps props, OpProps senderProps, OpProps receiverProps, BatchSchema schema, PhysicalOperator child, OptionManager optionManager) {
    super(props, child);
    this.senderProps = senderProps;
    this.receiverProps = receiverProps;
    this.schema = schema;
    this.optionManager = optionManager;
  }

  protected long computeMemReserve() {
    final boolean enableAccurateMemoryCalculation = optionManager.getOption(PlannerSettings.ENABLE_ACCURATE_MEMORY_ESTIMATION);
    if(enableAccurateMemoryCalculation && !isMemReserveUpdated && receiverLocations != null && receiverLocations.size() >= 1) {
      isMemReserveUpdated = true;

      //Memory required by HashPartitionSender depends on number of downstream receiver fragments
      props.setMemReserve(receiverLocations.size() * props.getMemReserve());
    }
    return props.getMemReserve();
  }

  /**
   * Default sender parallelization width range is unlimited
   */
  @Override
  public ParallelizationInfo.WidthConstraint getSenderParallelizationWidthConstraint() {
    return ParallelizationInfo.WidthConstraint.UNLIMITED;
  }

  /**
   * Default sender parallelization affinity is no endpoint affinity
   */
  @Override
  public Supplier<Collection<EndpointAffinity>> getSenderEndpointffinity(Supplier<Collection<NodeEndpoint>> receiverFragmentEndpointsSupplier) {
    return () -> ImmutableList.of();
  }

  /**
   * Default receiver parallelization width range is unlimited
   */
  @Override
  public ParallelizationInfo.WidthConstraint getReceiverParallelizationWidthConstraint() {
    return ParallelizationInfo.WidthConstraint.UNLIMITED;
  }

  /**
   * Default receiver parallelization is affinity to nodes where sender fragments are running.
   */
  @Override
  public Supplier<Collection<EndpointAffinity>> getReceiverEndpointAffinity(Supplier<Collection<NodeEndpoint>> senderFragmentEndpointsSupplier) {
    return () -> {
      Collection<NodeEndpoint> senderFragmentEndpoints = senderFragmentEndpointsSupplier.get();
      Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");
      return getDefaultAffinityMap(senderFragmentEndpoints);
    };
  }

  /**
   * Get a default endpoint affinity map where affinity of a SabotNode is proportional to the number of its occurrances
   * in given endpoint list.
   *
   * @param fragmentEndpoints SabotNode endpoint assignments of fragments.
   * @return List of EndpointAffinity objects for each SabotNode endpoint given <i>fragmentEndpoints</i>.
   */
  protected static List<EndpointAffinity> getDefaultAffinityMap(Collection<NodeEndpoint> fragmentEndpoints) {
    Map<NodeEndpoint, EndpointAffinity> affinityMap = Maps.newHashMap();
    final double affinityPerOccurrence = 1.0d / fragmentEndpoints.size();
    for(NodeEndpoint sender : fragmentEndpoints) {
      if (affinityMap.containsKey(sender)) {
        affinityMap.get(sender).addAffinity(affinityPerOccurrence);
      } else {
        affinityMap.put(sender, new EndpointAffinity(sender, affinityPerOccurrence));
      }
    }

    return new ArrayList<>(affinityMap.values());
  }

  protected void setupSenders(List<NodeEndpoint> senderLocations) throws PhysicalOperatorSetupException {
    this.senderLocations = ImmutableList.copyOf(senderLocations);
  }

  protected void setupReceivers(List<NodeEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverLocations = ImmutableList.copyOf(receiverLocations);
  }

  @Override
  public final void setupSenders(int majorFragmentId, List<NodeEndpoint> senderLocations) throws PhysicalOperatorSetupException {
    this.senderMajorFragmentId = majorFragmentId;
    setupSenders(senderLocations);
  }

  @Override
  public final void setupReceivers(int majorFragmentId, List<NodeEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverMajorFragmentId = majorFragmentId;
    setupReceivers(receiverLocations);
  }

  @Override
  public final <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitExchange(this, value);
  }

  @Override
  public int getOperatorType() {
    return Integer.MIN_VALUE;
  }

  @Override
  public ParallelizationDependency getParallelizationDependency() {
    return ParallelizationDependency.RECEIVER_DEPENDS_ON_SENDER;
  }

}
