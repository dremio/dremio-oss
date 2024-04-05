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

import static com.dremio.exec.store.schedule.HardAssignmentCreator.INSTANCE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.exec.ExecTest;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.google.common.base.Objects;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/** Tests for {@link HardAssignmentCreator} */
public class TestHardAssignmentCreator extends ExecTest {

  private static final NodeEndpoint ENDPOINT_1_1 = newNodeEndpoint("10.0.0.1", 1234);
  private static final NodeEndpoint ENDPOINT_1_2 = newNodeEndpoint("10.0.0.1", 4567);
  private static final NodeEndpoint ENDPOINT_2_1 = newNodeEndpoint("10.0.0.2", 1234);
  private static final NodeEndpoint ENDPOINT_2_2 = newNodeEndpoint("10.0.0.2", 4567);
  private static final NodeEndpoint ENDPOINT_3_1 = newNodeEndpoint("10.0.0.3", 1234);
  private static final NodeEndpoint ENDPOINT_3_2 = newNodeEndpoint("10.0.0.3", 4567);
  private static final NodeEndpoint ENDPOINT_4_1 = newNodeEndpoint("10.0.0.4", 4567);
  private static final NodeEndpoint ENDPOINT_4_2 = newNodeEndpoint("10.0.0.4", 8910);

  private static final List<NodeEndpoint> ENDPOINTS =
      asList(
          ENDPOINT_1_1,
          ENDPOINT_2_1,
          ENDPOINT_1_2,
          ENDPOINT_2_2,
          ENDPOINT_3_1,
          ENDPOINT_3_2,
          ENDPOINT_4_1,
          ENDPOINT_4_2);

  public static class TestWork implements CompleteWork {
    private final String id;
    private final long sizeInBytes;
    private final List<EndpointAffinity> affinities;

    public TestWork(String id, long sizeInBytes, List<EndpointAffinity> affinities) {
      this.id = id;
      this.sizeInBytes = sizeInBytes;
      this.affinities = affinities;
    }

    @Override
    public long getTotalBytes() {
      return sizeInBytes;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return Long.compare(getTotalBytes(), o.getTotalBytes());
    }

    @Override
    public List<EndpointAffinity> getAffinity() {
      return affinities;
    }

    public String getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestWork testWork = (TestWork) o;
      return sizeInBytes == testWork.sizeInBytes
          && Objects.equal(id, testWork.id)
          && Objects.equal(affinities, testWork.affinities);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, sizeInBytes, affinities);
    }

    @Override
    public String toString() {
      return "TestWorkUnit: id=" + id + ", affinity: " + affinities;
    }
  }

  private CompleteWork newWork(
      String id, long sizeInBytes, NodeEndpoint endpoint, double affinity) {
    return new TestWork(id, sizeInBytes, asList(new EndpointAffinity(endpoint, affinity, true, 1)));
  }

  @Test
  public void simpleOneFileOnAHost() throws Exception {
    final List<CompleteWork> workUnits =
        asList(newWork("/10.0.0.1/table/foo1", 1024, ENDPOINT_1_1, 1.00));

    ListMultimap<Integer, CompleteWork> mappings;
    List<NodeEndpoint> endpoints;

    endpoints = asList(ENDPOINT_1_1);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Pass another nodendpoint running on same host
    endpoints = asList(ENDPOINT_1_2);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Negative case - fails because there are no ENDPOINTS running on 10.0.0.1 in the assigned list
    verifyAssignmentFails(workUnits, ENDPOINT_2_1);

    // Negative case - fails because the assigned list contains more ENDPOINTS running on 10.0.0.1
    // than the files on
    // 10.0.0.1
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_1_2);
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_2_2);
  }

  @Test
  public void simpleTwoFileOneOnEachHost() throws Exception {
    final List<CompleteWork> workUnits =
        asList(
            newWork("/10.0.0.1/table/foo1", 1024, ENDPOINT_1_1, 0.33),
            newWork("/10.0.0.2/table/foo2", 2048, ENDPOINT_2_2, 0.66));

    ListMultimap<Integer, CompleteWork> mappings;
    List<NodeEndpoint> endpoints;

    endpoints = asList(ENDPOINT_1_1, ENDPOINT_2_2);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Pass different endpoint on the same host that is not passed in workUnit affinity list
    endpoints = asList(ENDPOINT_1_2, ENDPOINT_2_1);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    endpoints = asList(ENDPOINT_1_1, ENDPOINT_2_1);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    endpoints = asList(ENDPOINT_1_2, ENDPOINT_2_2);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Negative case - fails because there are no ENDPOINTS running on 10.0.0.2 in the assigned list
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_1_2);

    // Negative case - fails because there are no ENDPOINTS running on 10.0.0.2 in the assigned list
    verifyAssignmentFails(workUnits, ENDPOINT_1_1);

    // Negative case - fails because the assigned list contains more ENDPOINTS running on 10.0.0.1
    // than the files on
    // 10.0.0.1
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_1_2, ENDPOINT_2_1);
  }

  @Test
  public void twoFilesOnSameHost() throws Exception {
    final List<CompleteWork> workUnits =
        asList(
            newWork("/10.0.0.1/table/foo1", 1024, ENDPOINT_1_1, 0.33),
            newWork("/10.0.0.1/table/foo2", 2048, ENDPOINT_1_2, 0.66));

    ListMultimap<Integer, CompleteWork> mappings;
    List<NodeEndpoint> endpoints;

    endpoints = asList(ENDPOINT_1_1, ENDPOINT_1_2);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Assign only one endpoint from 10.0.0.1
    endpoints = asList(ENDPOINT_1_2);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Negative case - fails because the assigned list contains ENDPOINTS running on 10.0.0.2, but
    // there are no files on
    // 10.0.0.2
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_2_1);
  }

  @Test
  public void oneOrMoreFilesOnEachHost() throws Exception {
    final List<CompleteWork> workUnits =
        asList(
            newWork("/10.0.0.1/table/foo", 1024, ENDPOINT_1_1, 1024f / 48124f),
            newWork("/10.0.0.1/table/bar", 4096, ENDPOINT_1_2, 4096f / 48124f),
            newWork("/10.0.0.1/table/fb", 8192, ENDPOINT_1_2, 8192f / 48124f),
            newWork("/10.0.0.2/table/foo", 2048, ENDPOINT_2_2, 2048f / 48124f),
            newWork("/10.0.0.2/table/bar", 4096, ENDPOINT_2_1, 4096f / 48124f),
            newWork("/10.0.0.3/table/foo", 16384, ENDPOINT_3_1, 16384f / 48124f),
            newWork("/10.0.0.3/table/bar", 2046, ENDPOINT_3_2, 2046f / 48124f),
            newWork("/10.0.0.3/table/bar2", 6144, ENDPOINT_3_2, 6144f / 48124f),
            newWork("/10.0.0.3/table/bar3", 2046, ENDPOINT_3_2, 2046f / 48124f),
            newWork("/10.0.0.4/table/bar", 2046, ENDPOINT_4_1, 2046f / 48124f));

    ListMultimap<Integer, CompleteWork> mappings;
    List<NodeEndpoint> endpoints;

    endpoints = ENDPOINTS.subList(0, 7);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Assign one endpoint from each machine
    endpoints = asList(ENDPOINT_1_1, ENDPOINT_2_1, ENDPOINT_3_1, ENDPOINT_4_1);
    mappings = INSTANCE.getMappings(endpoints, workUnits);
    verifyAssignments(mappings, endpoints, workUnits);

    // Negative case - fails because the assigned list contains no ENDPOINTS running on 10.0.0.4
    verifyAssignmentFails(workUnits, ENDPOINT_1_1, ENDPOINT_2_1, ENDPOINT_3_1);

    // Negative case - fails because the assigned list contains more ENDPOINTS running on 10.0.0.4
    // than the number of
    // files on 10.0.0.4
    verifyAssignmentFails(
        workUnits, ENDPOINT_1_1, ENDPOINT_2_1, ENDPOINT_3_1, ENDPOINT_4_1, ENDPOINT_4_2);
  }

  private static final void verifyAssignments(
      ListMultimap<Integer, CompleteWork> assignments,
      List<NodeEndpoint> inputEps,
      List<CompleteWork> inputWorks) {
    final String summary = summary(assignments, inputEps, inputWorks);
    final Set<CompleteWork> assignedSet = Sets.newHashSet(assignments.values());
    for (CompleteWork cw : inputWorks) {
      assertTrue(
          "Input work not present in assigned work unit list: " + summary,
          assignedSet.contains(cw));
      assertTrue(summary, assignedSet.remove(cw));
    }

    assertTrue(
        "There are some extra works in assigned work unit list: " + summary,
        assignedSet.size() == 0);

    int i = 0;
    Set<CompleteWork> inputWorkSet = new HashSet<>(inputWorks);
    for (NodeEndpoint ep : inputEps) {
      Collection<CompleteWork> assignedWorks = assignments.get(i);
      for (CompleteWork assignedWork : assignedWorks) {
        assertEquals(
            "Wrong endpoint assigned: " + summary,
            ep.getAddress(),
            assignedWork.getAffinity().get(0).getEndpoint().getAddress());
        assertTrue(summary, inputWorkSet.remove(assignedWork));
      }
      i++;
    }
  }

  private static final String summary(
      ListMultimap<Integer, CompleteWork> assignments,
      List<NodeEndpoint> inputEps,
      List<CompleteWork> inputWorks) {
    return "Assignments: "
        + assignments
        + ", input endpoints: "
        + inputEps
        + ", input works: "
        + inputWorks;
  }

  private static final void verifyAssignmentFails(
      List<CompleteWork> workUnits, NodeEndpoint... endpoints) {
    try {
      INSTANCE.getMappings(asList(endpoints), workUnits);
      fail("Shouldn't have reached here");
    } catch (final Exception e) {
      // expected
    }
  }

  public static final NodeEndpoint newNodeEndpoint(String address, int port) {
    return NodeEndpoint.newBuilder().setAddress(address).setFabricPort(port).build();
  }
}
