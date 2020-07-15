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

import static com.dremio.exec.ExecConstants.SLICE_TARGET_DEFAULT;
import static com.dremio.exec.planner.fragment.HardAffinityFragmentParallelizer.INSTANCE;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.service.namespace.LegacyPartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.PartitionChunk;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestHardAffinityFragmentParallelizer {

  // Create a set of test endpoints
  private static final NodeEndpoint N1_EP1 = newNodeEndpoint("node1", 30010);
  private static final NodeEndpoint N1_EP2 = newNodeEndpoint("node1", 30011);
  private static final NodeEndpoint N2_EP1 = newNodeEndpoint("node2", 30010);
  private static final NodeEndpoint N2_EP2 = newNodeEndpoint("node2", 30011);
  private static final NodeEndpoint N3_EP1 = newNodeEndpoint("node3", 30010);
  private static final NodeEndpoint N3_EP2 = newNodeEndpoint("node3", 30011);
  private static final NodeEndpoint N4_EP2 = newNodeEndpoint("node4", 30011);

  private static final NodeEndpoint newNodeEndpoint(String address, int port) {
    return NodeEndpoint.newBuilder().setAddress(address).setFabricPort(port).build();
  }

  private static final ParallelizationParameters newParameters(final long threshold, final int maxWidthPerNode,
      final int maxGlobalWidth) {
    return new ParallelizationParameters() {
      @Override
      public long getSliceTarget() {
        return threshold;
      }

      @Override
      public int getMaxWidthPerNode() {
        return maxWidthPerNode;
      }

      @Override
      public int getMaxGlobalWidth() {
        return maxGlobalWidth;
      }

      /**
       * {@link HardAffinityFragmentParallelizer} doesn't use affinity factor.
       * @return
       */
      @Override
      public double getAffinityFactor() {
        return 0.0f;
      }

      @Override
      public boolean useNewAssignmentCreator() {
        return true;
      }

      @Override
      public double getAssignmentCreatorBalanceFactor() {
        return 1.5;
      }

      @Override
      public boolean shouldIgnoreLeafAffinity() {
        return false;
      }
    };
  }

  private final Wrapper newWrapper(double cost, int minWidth, int maxWidth, List<EndpointAffinity> epAffs) {
    PhysicalOperator root = Mockito.mock(PhysicalOperator.class);
    Fragment fragment = Mockito.mock(Fragment.class);
    when(fragment.getRoot()).thenReturn(root);
    final Wrapper fragmentWrapper = new Wrapper(fragment, 1);
    final Stats stats = fragmentWrapper.getStats();
    stats.addCost(cost);
    stats.addMinWidth(minWidth);
    stats.addMaxWidth(maxWidth);
    GroupScan groupScan = Mockito.mock(GroupScan.class);
    when(groupScan.getDistributionAffinity()).thenReturn(DistributionAffinity.HARD);
    stats.addSplits(groupScan, transform(epAffs));
    return fragmentWrapper;
  }

  private final Wrapper newSplitWrapper(double cost, int minWidth, int maxWidth, List<EndpointAffinity> epAffs,
      ExecutionNodeMap execNodeMap) {
    PhysicalOperator root = Mockito.mock(PhysicalOperator.class);
    Fragment fragment = Mockito.mock(Fragment.class);
    when(fragment.getRoot()).thenReturn(root);
    final Wrapper fragmentWrapper = new Wrapper(fragment, 1);
    final Stats stats = fragmentWrapper.getStats();
    stats.addCost(cost);
    stats.addMinWidth(minWidth);
    stats.addMaxWidth(maxWidth);
    final PartitionChunk partitionChunk = PartitionChunk.newBuilder().build();
    final DatasetSplit.Builder dataSplit = DatasetSplit.newBuilder();
    epAffs.forEach(endpointAffinity -> dataSplit.addAffinitiesBuilder()
        .setFactor(endpointAffinity.getAffinity())
        .setHost(endpointAffinity.getEndpoint().getAddress()));

    SplitWork splitWork = new SplitWork(new LegacyPartitionChunkMetadata(partitionChunk), dataSplit.build(), execNodeMap, DistributionAffinity.HARD);

    GroupScan groupScan = Mockito.mock(GroupScan.class);
    when(groupScan.getDistributionAffinity()).thenReturn(DistributionAffinity.HARD);
    stats.addSplits(groupScan, ImmutableList.<CompleteWork>of(splitWork));

    return fragmentWrapper;
  }

  private static class TestWorkUnit implements CompleteWork {

    private final List<EndpointAffinity> af;

    public TestWorkUnit(List<EndpointAffinity> af) {
      this.af = af;
    }

    @Override
    public int compareTo(CompleteWork o) {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public long getTotalBytes() {
      throw new UnsupportedOperationException("NYI");
    }

    @Override
    public List<EndpointAffinity> getAffinity() {
      return af;
    }

    @Override
    public String toString() {
      return "[TestWork: " + af.get(0).getEndpoint() + "]";
    }
  }

  private final List<CompleteWork> transform(final List<EndpointAffinity> epAffs) {
    List<CompleteWork> workUnits = Lists.newArrayList();
    for(EndpointAffinity epAff : epAffs) {
      for(int i = 0; i < epAff.getMaxWidth(); i++) {
        workUnits.add(new TestWorkUnit(asList(new EndpointAffinity(epAff.getEndpoint(), epAff.getAffinity(), true, 1))));
      }
    }
    return workUnits;
  }

  @Test
  public void simpleCase1() throws Exception {
    final Wrapper wrapper = newWrapper(200, 1, 20, Collections.singletonList(new EndpointAffinity(N1_EP1, 1.0, true, 50)));
    INSTANCE.parallelizeFragment(wrapper, newParameters(SLICE_TARGET_DEFAULT, 5, 20), null);

    // Expect the fragment parallelization to be just one because:
    // The cost (200) is below the threshold (SLICE_TARGET_DEFAULT) (which gives width of 200/10000 = ~1) and
    assertEquals(1, wrapper.getWidth());

    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(1, assignedEps.size());
    assertEquals(N1_EP1, assignedEps.get(0));
  }

  @Test
  public void matchHardAffinity() throws Exception {
    final Wrapper wrapper = newSplitWrapper(200, 1, 20,
        Collections.singletonList(new EndpointAffinity(N1_EP1, 1.0, true, 20)),
        new ExecutionNodeMap(ImmutableList.of(N1_EP1))
    );
    INSTANCE.parallelizeFragment(wrapper, newParameters(SLICE_TARGET_DEFAULT, 5, 20), null);
    assertEquals(1, wrapper.getWidth());

    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(1, assignedEps.size());
    assertEquals(N1_EP1, assignedEps.get(0));
  }

  @Test
  public void noMatchHardAffinity() throws Exception {
    try {
      final Wrapper wrapper = newSplitWrapper(200, 1, 20,
          Collections.singletonList(new EndpointAffinity(N1_EP1, 1.0, true, 20)),
          new ExecutionNodeMap(ImmutableList.of(N2_EP1))
      );

      INSTANCE.parallelizeFragment(wrapper, newParameters(SLICE_TARGET_DEFAULT, 5, 20),
        ImmutableList.of(N2_EP1));
      fail("Should throw exception as affinity endpoint does not match active endpoint");
    } catch (UserException uex) {
      assertTrue(uex.getMessage().startsWith("No executors are available for data with hard affinity."));
    }
  }

  @Test
  public void simpleCase2() throws Exception {
    // Set the slice target to 1
    final Wrapper wrapper = newWrapper(200, 1, 20, Collections.singletonList(new EndpointAffinity(N1_EP1, 1.0, true, 50)));
    INSTANCE.parallelizeFragment(wrapper, newParameters(1, 5, 20), null);

    // Expect the fragment parallelization to be 5:
    // 1. the cost (200) is above the threshold (SLICE_TARGET_DEFAULT) (which gives 200/1=200 width) and
    // 2. Max width per node is 5 (limits the width 200 to 5)
    assertEquals(5, wrapper.getWidth());

    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(5, assignedEps.size());
    for (NodeEndpoint ep : assignedEps) {
      assertEquals(N1_EP1, ep);
    }
  }

  @Test
  public void multiNodeCluster1() throws Exception {
    final Wrapper wrapper = newWrapper(200, 1, 20,
        ImmutableList.of(
            new EndpointAffinity(N1_EP1, 0.15, true, 50),
            new EndpointAffinity(N1_EP2, 0.15, true, 50),
            new EndpointAffinity(N2_EP1, 0.10, true, 50),
            new EndpointAffinity(N3_EP2, 0.20, true, 50),
            new EndpointAffinity(N4_EP2, 0.20, true, 50)
        ));
    INSTANCE.parallelizeFragment(wrapper, newParameters(SLICE_TARGET_DEFAULT, 5, 20), null);

    // Expect the fragment parallelization to be 5 because:
    // 1. The cost (200) is below the threshold (SLICE_TARGET_DEFAULT) (which gives width of 200/10000 = ~1) and
    // 2. Number of mandoatory node assignments are 5 which overrides the cost based width of 1.
    assertEquals(5, wrapper.getWidth());

    // As there are 5 required eps and the width is 5, everyone gets assigned 1.
    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(5, assignedEps.size());
    assertTrue(assignedEps.contains(N1_EP1));
    assertTrue(assignedEps.contains(N1_EP2));
    assertTrue(assignedEps.contains(N2_EP1));
    assertTrue(assignedEps.contains(N3_EP2));
    assertTrue(assignedEps.contains(N4_EP2));
  }

  @Test
  public void multiNodeCluster2() throws Exception {
    final Wrapper wrapper = newWrapper(200, 1, 20,
        ImmutableList.of(
            new EndpointAffinity(N1_EP2, 0.15, true, 50),
            new EndpointAffinity(N2_EP2, 0.15, true, 50),
            new EndpointAffinity(N3_EP1, 0.10, true, 50),
            new EndpointAffinity(N4_EP2, 0.20, true, 50),
            new EndpointAffinity(N1_EP1, 0.20, true, 50)
        ));
    INSTANCE.parallelizeFragment(wrapper, newParameters(1, 5, 20), null);

    // Expect the fragment parallelization to be 20 because:
    // 1. the cost (200) is above the threshold (SLICE_TARGET_DEFAULT) (which gives 200/1=200 width) and
    // 2. Number of mandatory node assignments are 5 (current width 200 satisfies the requirement)
    // 3. max fragment width is 20 which limits the width
    assertEquals(20, wrapper.getWidth());

    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(20, assignedEps.size());
    final HashMultiset<NodeEndpoint> counts = HashMultiset.create();
    for(final NodeEndpoint ep : assignedEps) {
      counts.add(ep);
    }
    // Each node gets at max 5.
    assertTrue(counts.count(N1_EP2) <= 5);
    assertTrue(counts.count(N2_EP2) <= 5);
    assertTrue(counts.count(N3_EP1) <= 5);
    assertTrue(counts.count(N4_EP2) <= 5);
    assertTrue(counts.count(N1_EP1) <= 5);
  }

  @Test
  public void multiNodeClusterNonNormalizedAffinities() throws Exception {
    final Wrapper wrapper = newWrapper(2000, 1, 250,
        ImmutableList.of(
            new EndpointAffinity(N1_EP2, 15, true, 50),
            new EndpointAffinity(N2_EP2, 15, true, 50),
            new EndpointAffinity(N3_EP1, 10, true, 50),
            new EndpointAffinity(N4_EP2, 20, true, 50),
            new EndpointAffinity(N1_EP1, 20, true, 50)
        ));
    INSTANCE.parallelizeFragment(wrapper, newParameters(100, 20, 80), null);

    // Expect the fragment parallelization to be 20 because:
    // 1. the cost (2000) is above the threshold (SLICE_TARGET_DEFAULT) (which gives 2000/100=20 width) and
    // 2. Number of mandatory node assignments are 5 (current width 200 satisfies the requirement)
    // 3. max width per node is 20 which limits the width to 100, but existing width (20) is already less
    assertEquals(20, wrapper.getWidth());

    final List<NodeEndpoint> assignedEps = wrapper.getAssignedEndpoints();
    assertEquals(20, assignedEps.size());
    final HashMultiset<NodeEndpoint> counts = HashMultiset.create();
    for(final NodeEndpoint ep : assignedEps) {
      counts.add(ep);
    }
    // Each node gets at max 5.
    assertThat(counts.count(N1_EP2), CoreMatchers.allOf(greaterThan(1), lessThanOrEqualTo(5)));
    assertThat(counts.count(N2_EP2), CoreMatchers.allOf(greaterThan(1), lessThanOrEqualTo(5)));
    assertThat(counts.count(N3_EP1), CoreMatchers.allOf(greaterThan(1), lessThanOrEqualTo(5)));
    assertThat(counts.count(N4_EP2), CoreMatchers.allOf(greaterThan(1), lessThanOrEqualTo(5)));
    assertThat(counts.count(N1_EP1), CoreMatchers.allOf(greaterThan(1), lessThanOrEqualTo(5)));
  }

  @Test
  public void multiNodeClusterNegative2() throws Exception {
    final Wrapper wrapper = newWrapper(200, 1, 3,
        ImmutableList.of(
            new EndpointAffinity(N1_EP2, 0.15, true, 50),
            new EndpointAffinity(N2_EP2, 0.15, true, 50),
            new EndpointAffinity(N3_EP1, 0.10, true, 50),
            new EndpointAffinity(N4_EP2, 0.20, true, 50),
            new EndpointAffinity(N1_EP1, 0.20, true, 50)
        ));

    try {
      INSTANCE.parallelizeFragment(wrapper, newParameters(1, 2, 2), null);
      fail("Expected an exception, because max fragment width (3) is less than the number of mandatory nodes (5)");
    } catch (Exception e) {
      // ok
    }
  }
}
