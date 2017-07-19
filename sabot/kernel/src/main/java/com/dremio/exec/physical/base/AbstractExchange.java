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
package com.dremio.exec.physical.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.PhysicalOperatorSetupException;
import com.dremio.exec.planner.fragment.ParallelizationInfo;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public abstract class AbstractExchange extends AbstractSingle implements Exchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractExchange.class);

  // Ephemeral info for generating execution fragments.
  protected int senderMajorFragmentId;
  protected int receiverMajorFragmentId;
  protected List<NodeEndpoint> senderLocations;
  protected List<NodeEndpoint> receiverLocations;
  private BatchSchema cachedSchema;

  public AbstractExchange(PhysicalOperator child) {
    super(child);
  }

  /**
   * Default sender parallelization width range is [1, Integer.MAX_VALUE] and no endpoint affinity
   * @param receiverFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list.
   * @return
   */
  @Override
  public ParallelizationInfo getSenderParallelizationInfo(List<NodeEndpoint> receiverFragmentEndpoints) {
    return ParallelizationInfo.UNLIMITED_WIDTH_NO_ENDPOINT_AFFINITY;
  }

  /**
   * Default receiver parallelization width range is [1, Integer.MAX_VALUE] and affinity to nodes where sender
   * fragments are running.
   * @param senderFragmentEndpoints Endpoints assigned to receiver fragment if available, otherwise an empty list.
   * @return
   */
  @Override
  public ParallelizationInfo getReceiverParallelizationInfo(List<NodeEndpoint> senderFragmentEndpoints) {
    Preconditions.checkArgument(senderFragmentEndpoints != null && senderFragmentEndpoints.size() > 0,
        "Sender fragment endpoint list should not be empty");

    return ParallelizationInfo.create(1, Integer.MAX_VALUE, getDefaultAffinityMap(senderFragmentEndpoints));
  }

  /**
   * Get a default endpoint affinity map where affinity of a SabotNode is proportional to the number of its occurrances
   * in given endpoint list.
   *
   * @param fragmentEndpoints SabotNode endpoint assignments of fragments.
   * @return List of EndpointAffinity objects for each SabotNode endpoint given <i>fragmentEndpoints</i>.
   */
  protected static List<EndpointAffinity> getDefaultAffinityMap(List<NodeEndpoint> fragmentEndpoints) {
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

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {

    // since we can do many levels of parallelization, ensure to cache schema locally.
    if(cachedSchema == null){
      cachedSchema = child.getSchema(context);
      if(cachedSchema == null){
        throw new UnsupportedOperationException();
      }
    }
    return cachedSchema;
  }
}
