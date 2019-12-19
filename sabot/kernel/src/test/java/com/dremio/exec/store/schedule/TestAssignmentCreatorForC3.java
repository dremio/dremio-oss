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
package com.dremio.exec.store.schedule;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;

/**
 * Tests for split assignments for cloud cache
 */
public class TestAssignmentCreatorForC3 extends ExecTest {
  // _1 and _2 are different instances on the same node
  private static final CoordinationProtos.NodeEndpoint ENDPOINT1_1 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.1", 1234);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT1_2 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.1", 4567);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT2_1 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.2", 1234);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT2_2 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.2", 4567);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT3_1 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.3", 1234);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT3_2 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.3", 4567);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT4_1 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.4", 1234);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT4_2 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.4", 4567);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT5_1 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.5", 1234);
  private static final CoordinationProtos.NodeEndpoint ENDPOINT5_2 = TestHardAssignmentCreator.newNodeEndpoint("10.0.0.5", 4567);

  private static final List<CoordinationProtos.NodeEndpoint> ENDPOINTS =
    asList(
      ENDPOINT1_1,
      ENDPOINT1_2,
      ENDPOINT2_1,
      ENDPOINT2_2,
      ENDPOINT3_1,
      ENDPOINT3_2,
      ENDPOINT4_1,
      ENDPOINT4_2,
      ENDPOINT5_1,
      ENDPOINT5_2
    );

  // Create 15 splits with different affinities
  // Tests will use different combinations of splits to assign to the end points
  // 5 splits each with each endpoint having highest affinity
  // EP1: S1, S6, S11
  // EP2: S2, S7, S12
  // EP3: S3, S8, S13
  // EP4: S4, S9, S14
  // EP5: S5, S10, S15
  private static final TestHardAssignmentCreator.TestWork S1 = newWork("S1", 10000, newAffinity(ENDPOINT1_1, 9000), newAffinity(ENDPOINT2_1, 8000), newAffinity(ENDPOINT3_1, 4000));
  private static final TestHardAssignmentCreator.TestWork S2 = newWork("S2", 9992, newAffinity(ENDPOINT2_1, 9500), newAffinity(ENDPOINT3_1, 8000), newAffinity(ENDPOINT4_1, 6200));
  private static final TestHardAssignmentCreator.TestWork S3 = newWork("S3", 9990, newAffinity(ENDPOINT3_1, 8000), newAffinity(ENDPOINT4_1, 7500), newAffinity(ENDPOINT5_1, 5000));
  private static final TestHardAssignmentCreator.TestWork S4 = newWork("S4", 9980, newAffinity(ENDPOINT4_1, 8500), newAffinity(ENDPOINT5_1, 8200), newAffinity(ENDPOINT1_1, 3000));
  private static final TestHardAssignmentCreator.TestWork S5 = newWork("S5", 9800, newAffinity(ENDPOINT5_1, 9000), newAffinity(ENDPOINT1_1, 8000), newAffinity(ENDPOINT2_1, 4500));
  private static final TestHardAssignmentCreator.TestWork S6 = newWork("S6", 9778, newAffinity(ENDPOINT1_1, 8500), newAffinity(ENDPOINT3_1, 7000), newAffinity(ENDPOINT5_1, 5000));
  private static final TestHardAssignmentCreator.TestWork S7 = newWork("S7", 9776, newAffinity(ENDPOINT2_1, 8600), newAffinity(ENDPOINT4_1, 8000), newAffinity(ENDPOINT1_1, 4000));
  private static final TestHardAssignmentCreator.TestWork S8 = newWork("S8", 9774, newAffinity(ENDPOINT3_1, 8700), newAffinity(ENDPOINT5_1, 7000), newAffinity(ENDPOINT2_1, 4500));
  private static final TestHardAssignmentCreator.TestWork S9 = newWork("S9", 9770, newAffinity(ENDPOINT4_1, 8400), newAffinity(ENDPOINT1_1, 6000), newAffinity(ENDPOINT3_1, 4000));
  private static final TestHardAssignmentCreator.TestWork S10 = newWork("S10", 9700, newAffinity(ENDPOINT5_1, 9000), newAffinity(ENDPOINT2_1, 6200), newAffinity(ENDPOINT4_1, 5000));
  private static final TestHardAssignmentCreator.TestWork S11 = newWork("S11", 9680, newAffinity(ENDPOINT1_2, 8800), newAffinity(ENDPOINT4_2, 6000), newAffinity(ENDPOINT2_2, 3000));
  private static final TestHardAssignmentCreator.TestWork S12 = newWork("S12", 9600, newAffinity(ENDPOINT2_2, 8000), newAffinity(ENDPOINT5_2, 7500), newAffinity(ENDPOINT3_2, 5000));
  private static final TestHardAssignmentCreator.TestWork S13 = newWork("S13", 9500, newAffinity(ENDPOINT3_2, 9500), newAffinity(ENDPOINT1_2, 5000), newAffinity(ENDPOINT4_2, 2800));
  private static final TestHardAssignmentCreator.TestWork S14 = newWork("S14", 9450, newAffinity(ENDPOINT4_2, 8000), newAffinity(ENDPOINT2_2, 5900), newAffinity(ENDPOINT5_2, 3300));
  private static final TestHardAssignmentCreator.TestWork S15 = newWork("S15", 9400, newAffinity(ENDPOINT5_2, 9000), newAffinity(ENDPOINT3_2, 5000), newAffinity(ENDPOINT1_2, 4800));
  private static final TestHardAssignmentCreator.TestWork S16 = newWork("S16", 9390, newAffinity(ENDPOINT1_2, 9200), newAffinity(ENDPOINT5_2, 7200), newAffinity(ENDPOINT4_2, 3600));
  private static final TestHardAssignmentCreator.TestWork S17 = newWork("S17", 9380, newAffinity(ENDPOINT2_2, 9000), newAffinity(ENDPOINT1_2, 6800), newAffinity(ENDPOINT5_2, 4000));
  private static final TestHardAssignmentCreator.TestWork S18 = newWork("S18", 9376, newAffinity(ENDPOINT3_2, 8800), newAffinity(ENDPOINT2_2, 6200), newAffinity(ENDPOINT1_2, 3800));
  private static final TestHardAssignmentCreator.TestWork S19 = newWork("S19", 9372, newAffinity(ENDPOINT4_2, 8600), newAffinity(ENDPOINT3_2, 5800), newAffinity(ENDPOINT2_2, 2400));
  private static final TestHardAssignmentCreator.TestWork S20 = newWork("S20", 9370, newAffinity(ENDPOINT5_2, 8770), newAffinity(ENDPOINT4_2, 7000), newAffinity(ENDPOINT3_2, 3000));

  // splits with only one affinity. These can get assigned randomly
  private static final TestHardAssignmentCreator.TestWork S101 = newWork("S101", 10000, newAffinity(ENDPOINT1_1, 10000));
  private static final TestHardAssignmentCreator.TestWork S102 = newWork("S102", 9900, newAffinity(ENDPOINT1_1, 9000));

  // same splits as above, but with instance affinity
  private static final TestHardAssignmentCreator.TestWork Instance_S1 = cloneInstanceWork(S1);
  private static final TestHardAssignmentCreator.TestWork Instance_S2 = cloneInstanceWork(S2);
  private static final TestHardAssignmentCreator.TestWork Instance_S3 = cloneInstanceWork(S3);
  private static final TestHardAssignmentCreator.TestWork Instance_S4 = cloneInstanceWork(S4);
  private static final TestHardAssignmentCreator.TestWork Instance_S5 = cloneInstanceWork(S5);
  private static final TestHardAssignmentCreator.TestWork Instance_S6 = cloneInstanceWork(S6);
  private static final TestHardAssignmentCreator.TestWork Instance_S7 = cloneInstanceWork(S7);
  private static final TestHardAssignmentCreator.TestWork Instance_S8 = cloneInstanceWork(S8);
  private static final TestHardAssignmentCreator.TestWork Instance_S9 = cloneInstanceWork(S9);
  private static final TestHardAssignmentCreator.TestWork Instance_S10 = cloneInstanceWork(S10);
  private static final TestHardAssignmentCreator.TestWork Instance_S11 = cloneInstanceWork(S11);
  private static final TestHardAssignmentCreator.TestWork Instance_S12 = cloneInstanceWork(S12);
  private static final TestHardAssignmentCreator.TestWork Instance_S13 = cloneInstanceWork(S13);
  private static final TestHardAssignmentCreator.TestWork Instance_S14 = cloneInstanceWork(S14);
  private static final TestHardAssignmentCreator.TestWork Instance_S15 = cloneInstanceWork(S15);
  private static final TestHardAssignmentCreator.TestWork Instance_S16 = cloneInstanceWork(S16);
  private static final TestHardAssignmentCreator.TestWork Instance_S17 = cloneInstanceWork(S17);
  private static final TestHardAssignmentCreator.TestWork Instance_S18 = cloneInstanceWork(S18);
  private static final TestHardAssignmentCreator.TestWork Instance_S19 = cloneInstanceWork(S19);
  private static final TestHardAssignmentCreator.TestWork Instance_S20 = cloneInstanceWork(S20);

  private static EndpointAffinity newAffinity(CoordinationProtos.NodeEndpoint endpoint, double affinity) {
    return new EndpointAffinity(endpoint, affinity);
  }

  private static EndpointAffinity newInstanceAffinity(CoordinationProtos.NodeEndpoint endpoint, double affinity) {
    return new EndpointAffinity(HostAndPort.fromParts(endpoint.getAddress(), endpoint.getFabricPort()), endpoint, affinity, true, Integer.MAX_VALUE);
  }

  private static TestHardAssignmentCreator.TestWork cloneInstanceWork(TestHardAssignmentCreator.TestWork work) {
    List<EndpointAffinity> affinityList = Lists.newArrayList();
    for(EndpointAffinity affinity : work.getAffinity()) {
      affinityList.add(newInstanceAffinity(affinity.getEndpoint(), affinity.getAffinity()));
    }

    String name = "Instance_" + work.getId();
    return new TestHardAssignmentCreator.TestWork(name, work.getTotalBytes(), affinityList);
  }

  private static TestHardAssignmentCreator.TestWork newWork(String id, long sizeInBytes, EndpointAffinity... affinityList) {
    return new TestHardAssignmentCreator.TestWork(id, sizeInBytes, asList(affinityList));
  }

  void verifyMappings(List<CoordinationProtos.NodeEndpoint> endpoints, Multimap<Integer, TestHardAssignmentCreator.TestWork> mappings, Map<String, String> expectedMappings) {
    boolean isInstanceAffinity = mappings.values().iterator().next().getAffinity().get(0).isInstanceAffinity();
    Map<Integer, String> fragmentIdToHostnameMap = Maps.newHashMap();
    int fragmentId = 0;
    for(CoordinationProtos.NodeEndpoint endpoint : endpoints) {
      fragmentIdToHostnameMap.put(fragmentId, getHostname(endpoint, isInstanceAffinity));
      fragmentId++;
    }

    for(Integer id : mappings.keySet()) {
      String hostname = fragmentIdToHostnameMap.get(id);
      for(TestHardAssignmentCreator.TestWork split : mappings.get(id)) {
        // This split is assigned to fragmentId id
        String splitId = split.getId();
        String expValue = expectedMappings.get(splitId);
        Assert.assertTrue("Split " + splitId + " should be assigned to " + expValue + " was assigned to " + hostname, expValue.equalsIgnoreCase(hostname));
      }
    }
  }

  void assignAndVerifySplits(List<CoordinationProtos.NodeEndpoint> endpoints, List<TestHardAssignmentCreator.TestWork> splits, Map<String, String> expectedMappings) throws Exception {
    verifyMappings(
      endpoints,
      AssignmentCreator2.getMappings(endpoints, splits, 1.5),
      expectedMappings);
  }

  String getHostname(CoordinationProtos.NodeEndpoint endpoint, boolean isInstanceAffinity) {
    if (!isInstanceAffinity) {
      return endpoint.getAddress();
    }

    return endpoint.getAddress() + ':' + endpoint.getFabricPort();
  }

  Map<String, String> assignSplitToHighestAffinity(List<TestHardAssignmentCreator.TestWork> splits) {
    Map<String, String> results = Maps.newHashMap();

    for(TestHardAssignmentCreator.TestWork split : splits) {
      String id = split.getId();

      double affinity = 0;
      String hostname = null;
      for(EndpointAffinity endpointAffinity : split.getAffinity()) {
        if (endpointAffinity.getAffinity() >= affinity) {
          hostname = getHostname(endpointAffinity.getEndpoint(), endpointAffinity.isInstanceAffinity());
          affinity = endpointAffinity.getAffinity();
        }
      }

      Assert.assertTrue(hostname != null);
      results.put(id, hostname);
    }

    return results;
  }

  @Test
  public void testNoSplits() {
    List<TestHardAssignmentCreator.TestWork> splits = new ArrayList<>();
    List<CoordinationProtos.NodeEndpoint> nodes = Lists.newArrayList(ENDPOINT1_1);
    ListMultimap mappings = AssignmentCreator2.getMappings(nodes, splits, 0.00);
    Assert.assertNotNull(mappings);
  }

  @Test
  public void testAllSplits() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20);
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      assignSplitToHighestAffinity(splits));
  }

  @Test
  public void testAllInstanceSplits() throws Exception {
    // instance affinities
    List<TestHardAssignmentCreator.TestWork> splits = asList(
      Instance_S1, Instance_S2, Instance_S3, Instance_S4, Instance_S5,
      Instance_S6, Instance_S7, Instance_S8, Instance_S9, Instance_S10,
      Instance_S11, Instance_S12, Instance_S13, Instance_S14, Instance_S15,
      Instance_S16, Instance_S17, Instance_S18, Instance_S19, Instance_S20);
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      assignSplitToHighestAffinity(splits));
  }

  @Test
  public void testNoSplitsToEndpoint5() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S1, S2, S3, S4, S6, S7, S8, S9, S11, S12, S13, S14, S16, S17, S18, S19);
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      assignSplitToHighestAffinity(splits));
  }

  @Test
  public void testNoInstanceSplitsToEndpoint5() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(
      Instance_S1, Instance_S2, Instance_S3, Instance_S4,
      Instance_S6, Instance_S7, Instance_S8, Instance_S9,
      Instance_S11, Instance_S12, Instance_S13, Instance_S14,
      Instance_S16, Instance_S17, Instance_S18, Instance_S19);
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      assignSplitToHighestAffinity(splits));
  }

  @Test
  public void testSplitsWithAffinityToEndpoint1() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S1, S6, S11, S16);
    Map<String, String> expectedAssignments = Maps.newHashMap();
    expectedAssignments.put(S1.getId(), ENDPOINT1_1.getAddress());
    // there are 2 fragments per node. S6 gets assigned to the 2nd fragment
    expectedAssignments.put(S6.getId(), ENDPOINT1_2.getAddress());
    // These should get assigned to the node with the 2nd highest affinity
    expectedAssignments.put(S11.getId(), ENDPOINT4_1.getAddress());
    expectedAssignments.put(S16.getId(), ENDPOINT5_1.getAddress());

    // uses all endpoints - hence each node gets 2 fragments
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      expectedAssignments);
  }

  @Test
  public void testSplitsWithInstanceAffinityToEndpoint1() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(Instance_S1, Instance_S6, Instance_S11, Instance_S16);
    Map<String, String> expectedAssignments = Maps.newHashMap();
    expectedAssignments.put(Instance_S1.getId(), getHostname(ENDPOINT1_1, true));
    expectedAssignments.put(Instance_S11.getId(), getHostname(ENDPOINT1_2, true));
    // These should get assigned to the node with the 2nd highest affinity
    expectedAssignments.put(Instance_S6.getId(), getHostname(ENDPOINT3_1, true));
    expectedAssignments.put(Instance_S16.getId(), getHostname(ENDPOINT5_2, true));

    // uses all endpoints - hence each node gets 2 fragments
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      expectedAssignments);
  }

  @Test
  public void testSplitsWithAffinityToEndpoint1OneFragment() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S1, S6, S11, S16);
    Map<String, String> expectedAssignments = Maps.newHashMap();
    expectedAssignments.put(S1.getId(), ENDPOINT1_1.getAddress());
    // These should get assigned to the node with the 2nd highest affinity
    expectedAssignments.put(S6.getId(), ENDPOINT3_1.getAddress());
    expectedAssignments.put(S11.getId(), ENDPOINT4_1.getAddress());
    expectedAssignments.put(S16.getId(), ENDPOINT5_1.getAddress());

    assignAndVerifySplits(
      asList(ENDPOINT1_1, ENDPOINT2_1, ENDPOINT3_1, ENDPOINT4_1, ENDPOINT5_1),
      splits,
      expectedAssignments);
  }

  @Test
  public void testSplitsWithInstanceAffinityToEndpoint1OneFragment() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(Instance_S1, Instance_S6, Instance_S11, Instance_S16);
    Map<String, String> expectedAssignments = Maps.newHashMap();
    expectedAssignments.put(Instance_S1.getId(), getHostname(ENDPOINT1_1, true));
    // These should get assigned to the node with the 2nd highest affinity
    expectedAssignments.put(Instance_S6.getId(), getHostname(ENDPOINT3_1, true));
    expectedAssignments.put(Instance_S11.getId(), getHostname(ENDPOINT4_2, true));
    expectedAssignments.put(Instance_S16.getId(), getHostname(ENDPOINT5_2, true));

    assignAndVerifySplits(
      asList(ENDPOINT1_1, ENDPOINT2_1, ENDPOINT3_1, ENDPOINT4_2, ENDPOINT5_2),
      splits,
      expectedAssignments);
  }

  @Test
  public void testFilterOutOneSplit() throws Exception {
    List<TestHardAssignmentCreator.TestWork> masterList = asList(S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20);
    int i = 0;
    while (i < masterList.size()) {
      List<TestHardAssignmentCreator.TestWork> splits = Lists.newArrayList(masterList);
      splits.remove(i);
      assignAndVerifySplits(
        ENDPOINTS,
        splits,
        assignSplitToHighestAffinity(splits));
      i++;
    }
  }

  @Test
  public void testFilterOutOneSplitWithInstanceAffinity() throws Exception {
    List<TestHardAssignmentCreator.TestWork> masterList = asList(
      Instance_S1, Instance_S2, Instance_S3, Instance_S4, Instance_S5,
      Instance_S6, Instance_S7, Instance_S8, Instance_S9, Instance_S10,
      Instance_S11, Instance_S12, Instance_S13, Instance_S14, Instance_S15,
      Instance_S16, Instance_S17, Instance_S18, Instance_S19, Instance_S20);
    int i = 0;
    while (i < masterList.size()) {
      List<TestHardAssignmentCreator.TestWork> splits = Lists.newArrayList(masterList);
      splits.remove(i);
      assignAndVerifySplits(
        ENDPOINTS,
        splits,
        assignSplitToHighestAffinity(splits));
      i++;
    }
  }

  @Test
  public void testNodeFailure() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S1, S2, S3, S4, S6, S7, S8, S9, S11, S12, S13, S14, S16, S17, S18, S19);
    Map<String, String> expectedMappings = assignSplitToHighestAffinity(splits);
    // Since endpoint 1 is down, these get assigned to the 2nd highest affinity
    expectedMappings.put(S1.getId(), ENDPOINT2_1.getAddress());
    expectedMappings.put(S6.getId(), ENDPOINT3_1.getAddress());
    expectedMappings.put(S11.getId(), ENDPOINT4_1.getAddress());
    expectedMappings.put(S16.getId(), ENDPOINT5_1.getAddress());
    assignAndVerifySplits(
      asList(ENDPOINT2_1, ENDPOINT2_2, ENDPOINT3_1, ENDPOINT3_2, ENDPOINT4_1, ENDPOINT4_2, ENDPOINT5_1, ENDPOINT5_2),
      splits,
      expectedMappings);
  }

  @Test
  public void testNodeFailureWithInstanceAffinity() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(
      Instance_S1, Instance_S2, Instance_S3, Instance_S4,
      Instance_S6, Instance_S7, Instance_S8, Instance_S9,
      Instance_S11, Instance_S12, Instance_S13, Instance_S14,
      Instance_S16, Instance_S17, Instance_S18, Instance_S19);
    Map<String, String> expectedMappings = assignSplitToHighestAffinity(splits);
    // Since endpoint 1 is down, these get assigned to the 2nd highest affinity
    expectedMappings.put(Instance_S1.getId(), getHostname(ENDPOINT2_1, true));
    expectedMappings.put(Instance_S6.getId(), getHostname(ENDPOINT3_1, true));
    expectedMappings.put(Instance_S11.getId(), getHostname(ENDPOINT4_2, true));
    expectedMappings.put(Instance_S16.getId(), getHostname(ENDPOINT5_2, true));
    assignAndVerifySplits(
      asList(ENDPOINT2_1, ENDPOINT2_2, ENDPOINT3_1, ENDPOINT3_2, ENDPOINT4_1, ENDPOINT4_2, ENDPOINT5_1, ENDPOINT5_2),
      splits,
      expectedMappings);
  }

  @Test
  public void testUnAssignedSplits() throws Exception {
    List<TestHardAssignmentCreator.TestWork> splits = asList(S101, S102);
    assignAndVerifySplits(
      ENDPOINTS,
      splits,
      assignSplitToHighestAffinity(splits)
    );

    // there is no affinity in the assignments. Should get assigned after 3 passes via the unassigned list
    AssignmentCreator2.LEFTOVER_ASSIGNMENTS.set(0);
    AssignmentCreator2.getMappings(
      asList(ENDPOINT2_1, ENDPOINT3_1, ENDPOINT4_1, ENDPOINT5_1),
      splits,
      1.5
    );
    Assert.assertTrue("assignLeftOvers should have been invoked", AssignmentCreator2.LEFTOVER_ASSIGNMENTS.get() == 1);
  }

  class SplitGenerator {
    List<CoordinationProtos.NodeEndpoint> endpoints;
    Random rand;
    Iterator<Long> longIterator;
    int splitNum;
    long minSize = 10 * 1024 * 1024; // 10MB
    long maxSize = 1024 * 1024 * 1024; // 1GB

    SplitGenerator(int numNodes) {
      this.endpoints = Lists.newArrayList();
      this.splitNum = 0;
      this.rand = new Random();
      this.longIterator = this.rand.longs(minSize, maxSize).iterator();
      initNodes(numNodes);
    }

    void initNodes(int numNodes) {
      for(int i = 0; i < numNodes; i++) {
        String hostname = "10.0.0." + numNodes;
        this.endpoints.add(TestHardAssignmentCreator.newNodeEndpoint(hostname, 1234));
      }
    }

    TestHardAssignmentCreator.TestWork generateSplit() {
      String name = "S" + this.splitNum++;
      long numBytes = longIterator.next();
      Iterator<Long> affinityIterator = this.rand.longs(numBytes / 8, numBytes).iterator();
      List<EndpointAffinity> endpointAffinityList = Lists.newArrayList();
      for(int i = 0; i < 3; i++) {
        int endPointNum = rand.nextInt(endpoints.size());
        long affinity = affinityIterator.next();
        endpointAffinityList.add(newAffinity(endpoints.get(endPointNum), (double)affinity));
      }

      return new TestHardAssignmentCreator.TestWork(name, numBytes, endpointAffinityList);
    }

    List<CoordinationProtos.NodeEndpoint> getEndpoints() {
      return this.endpoints;
    }

    List<TestHardAssignmentCreator.TestWork> generateSplits(int numSplits) {
      List<TestHardAssignmentCreator.TestWork> splits = Lists.newArrayList();

      for(int i = 0; i < numSplits; i++) {
        splits.add(generateSplit());
      }

      return splits;
    }
  }

  long timeAssignments(int numSplits, int numNodes, int numFragments) {
    SplitGenerator generator = new SplitGenerator(numNodes);
    List<CoordinationProtos.NodeEndpoint> endpoints = Lists.newArrayList(generator.getEndpoints());
    List<TestHardAssignmentCreator.TestWork> splits = generator.generateSplits(numSplits);

    List<CoordinationProtos.NodeEndpoint> fragments = Lists.newArrayList();
    for(int i = 0; i < numFragments; i++) {
      fragments.addAll(endpoints);
    }

    long startTime = System.currentTimeMillis();
    AssignmentCreator2.getMappings(fragments, splits, 1.5);
    return System.currentTimeMillis() - startTime;
  }

  long testManyAssignments(int numAssignments, int numSplits, int numNodes, int numFragments) {
    long elapsedTime = 0;

    for(int i = 0; i < numAssignments; i++) {
      elapsedTime += timeAssignments(numSplits, numNodes, numFragments);
    }

    return elapsedTime / numAssignments;
  }

  @Test
  public void timedTestAssignments() throws Exception {
    int[] splitsArr = new int[] {
      20000,
      50000
    };

    for(int i = 0; i < splitsArr.length; i++) {
      long timeTaken = testManyAssignments(5, splitsArr[i], 20, 8);
      System.out.println("Avg time taken for assigning " + splitsArr[i] + " splits is " + timeTaken + "ms");
    }
  }
}
