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
package com.dremio.exec.util;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocations;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;

/**
 * Tests for RendezvousSplitHasher
 */
public class SplitHasherTest {
  @Test
  public void testSplitHasherWithShuffledPaths() {
    verifySplitHasherWithShuffledPaths(1, 100);
    verifySplitHasherWithShuffledPaths(100, 1);
    verifySplitHasherWithShuffledPaths(10, 100);
    verifySplitHasherWithShuffledPaths(100, 1000);
  }

  @Test
  public void testSplitHasherWithDifferentEndPoints() {
    verifySplitHasherWithDifferentEndPoints(3, 100);
    verifySplitHasherWithDifferentEndPoints(100, 1);
    verifySplitHasherWithDifferentEndPoints(10, 100);
    verifySplitHasherWithDifferentEndPoints(100, 1000);
  }

  @Test
  public void testFallbackToRendezvousWhenBlockLocationsIsNull() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher spyHasher = Mockito.spy(new SplitHasher(minorFragmentEndpoints));
    List<String> paths = getSplitPaths(1);
    Path path = Path.of(paths.get(0));
    int offset = 0;
    spyHasher.getMinorFragmentIndex(path, null, offset, 256);
    verify(spyHasher, times(1)).getMinorFragmentIndexUsingRendezvousHash(path, offset);
  }

  @Test
  public void testFallbackToRendezvousWhenBlockLocationsIsEmpty() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher spyHasher = Mockito.spy(new SplitHasher(minorFragmentEndpoints));
    List<String> paths = getSplitPaths(1);
    Path path = Path.of(paths.get(0));
    int offset = 0;
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder().build();
    spyHasher.getMinorFragmentIndex(path, blockLocationsList, offset, 256);
    verify(spyHasher, times(1)).getMinorFragmentIndexUsingRendezvousHash(path, offset);
  }

  @Test
  public void testFallbackToRendezvousWhenNoAffinitiesPresentForCurrentSplit() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher spyHasher = Mockito.spy(new SplitHasher(minorFragmentEndpoints));
    List<String> paths = getSplitPaths(1);
    Path path = Path.of(paths.get(0));
    int offset = 0;

    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(256).setSize(256)
      .addAllHosts(Arrays.asList("host0", "host1", "host2"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(singletonList(blockLocations))
      .build();
    spyHasher.getMinorFragmentIndex(path, blockLocationsList, offset, 256);
    verify(spyHasher, times(1)).getMinorFragmentIndexUsingRendezvousHash(path, offset);
  }

  @Test
  public void testFallbackToRendezvousWhenNoMatchingExecutorsFound() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher spyHasher = Mockito.spy(new SplitHasher(minorFragmentEndpoints));
    List<String> paths = getSplitPaths(1);
    Path path = Path.of(paths.get(0));
    int offset = 0;
    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(0).setSize(256).addAllHosts(singletonList("some-random-host")).build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder().addBlockLocations(blockLocations).build();
    spyHasher.getMinorFragmentIndex(path, blockLocationsList, offset, 256);
    verify(spyHasher, times(1)).getMinorFragmentIndexUsingRendezvousHash(path, offset);
  }

  @Test
  public void testMinorFragmentAssignment_singleExecutor() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations1 = BlockLocations.newBuilder().setOffset(0).setSize(128)
      .addAllHosts(Arrays.asList("host0", "host1", "host2"))
      .build();
    BlockLocations blockLocations2 = BlockLocations.newBuilder().setOffset(128).setSize(256)
      .addAllHosts(Arrays.asList("host1", "host2"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(Arrays.asList(blockLocations1, blockLocations2))
      .build();

    assertEquals(0, hasher.getMinorFragmentIndex(null, blockLocationsList, 0, 256));
    assertEquals(0, hasher.getMinorFragmentIndex(null, blockLocationsList, 56, 256));
  }

  @Test
  public void testMinorFragmentAssignmentYarn_singleExecutor() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(1);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations1 = BlockLocations.newBuilder().setOffset(0).setSize(128)
      .addAllHosts(Arrays.asList("host0:9047", "host1:9047", "host2:9047"))
      .build();
    BlockLocations blockLocations2 = BlockLocations.newBuilder().setOffset(128).setSize(128)
      .addAllHosts(Arrays.asList("host1:9047", "host2:9047"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(Arrays.asList(blockLocations1, blockLocations2))
      .build();

    assertEquals(0, hasher.getMinorFragmentIndex(null, blockLocationsList, 0, 256));
    assertEquals(0, hasher.getMinorFragmentIndex(null, blockLocationsList, 56, 256));
  }

  @Test
  public void testMinorFragmentAssignmentToHighestAffinityExecutor() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(3);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations1 = BlockLocations.newBuilder().setOffset(0).setSize(128)
      .addAllHosts(Arrays.asList("some-random-host", "host1", "host2"))
      .build();
    BlockLocations blockLocations2 = BlockLocations.newBuilder().setOffset(128).setSize(128)
      .addAllHosts(Arrays.asList("some-random-host", "host1"))
      .build();

    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(Arrays.asList(blockLocations1, blockLocations2))
      .build();
    assertEquals(1, hasher.getMinorFragmentIndex(null, blockLocationsList, 56, 256));
    assertEquals(1, hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256));
  }

  @Test
  public void testMinorFragmentAssignmentToHighestAffinityExecutor_yarn() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(3);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations1 = BlockLocations.newBuilder().setOffset(0).setSize(128)
      .addAllHosts(Arrays.asList("some-random-host:9047", "host1:9047", "host2:9047"))
      .build();
    BlockLocations blockLocations2 = BlockLocations.newBuilder().setOffset(128).setSize(128)
      .addAllHosts(Arrays.asList("some-random-host:9047", "host1:9047"))
      .build();

    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(Arrays.asList(blockLocations1, blockLocations2))
      .build();
    assertEquals(1, hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256));
  }

  @Test
  public void testLoadDistribution_twoExecutorsWithEqualAffinities() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(2);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(0).setSize(256)
      .addAllHosts(Arrays.asList("host0", "host1"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(singletonList(blockLocations))
      .build();

    int timesAssignedFragment0 = 0;
    int timesAssignedFragment1 = 0;
    int numInvocations = 100;
    for (int i = 0; i < numInvocations; i++) {
      int assignedMinorFragment = hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256);
      if (assignedMinorFragment == 0) {
        timesAssignedFragment0++;
      } else {
        timesAssignedFragment1++;
      }
    }
    assertEquals(numInvocations / 2, timesAssignedFragment0);
    assertEquals(numInvocations / 2, timesAssignedFragment1);
  }

  @Test
  public void testLoadDistributionYarn_twoExecutorsWithEqualAffinities() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(2);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(0).setSize(256)
      .addAllHosts(Arrays.asList("host0:9047", "host1:9047"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(singletonList(blockLocations))
      .build();

    int timesAssignedFragment0 = 0;
    int timesAssignedFragment1 = 0;
    int numInvocations = 100;
    for (int i = 0; i < numInvocations; i++) {
      int assignedMinorFragment = hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256);
      if (assignedMinorFragment == 0) {
        timesAssignedFragment0++;
      } else {
        timesAssignedFragment1++;
      }
    }
    assertEquals(numInvocations / 2, timesAssignedFragment0);
    assertEquals(numInvocations / 2, timesAssignedFragment1);
  }

  @Test
  public void testLoadDistribution_threeExecutorsWithEqualAffinities() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(3);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(0).setSize(256)
      .addAllHosts(Arrays.asList("host0", "host1", "host2"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(singletonList(blockLocations))
      .build();

    int timesAssignedFragment0 = 0;
    int timesAssignedFragment1 = 0;
    int timesAssignedFragment2 = 0;
    int numInvocations = 9;
    for (int i = 0; i < numInvocations; i++) {
      int assignedMinorFragment = hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256);
      if (assignedMinorFragment == 0) {
        timesAssignedFragment0++;
      } else if (assignedMinorFragment == 1) {
        timesAssignedFragment1++;
      } else {
        timesAssignedFragment2++;
      }
    }
    assertEquals(numInvocations / 3, timesAssignedFragment0);
    assertEquals(numInvocations / 3, timesAssignedFragment1);
    assertEquals(numInvocations / 3, timesAssignedFragment2);
  }

  @Test
  public void testLoadDistributionYarn_threeExecutorsWithEqualAffinities() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(3);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations = BlockLocations.newBuilder().setOffset(0).setSize(256)
      .addAllHosts(Arrays.asList("host0:9047", "host1:9047", "host2:9047"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(singletonList(blockLocations))
      .build();

    int timesAssignedFragment0 = 0;
    int timesAssignedFragment1 = 0;
    int timesAssignedFragment2 = 0;
    int numInvocations = 9;
    for (int i = 0; i < numInvocations; i++) {
      int assignedMinorFragment = hasher.getMinorFragmentIndex(null, blockLocationsList, 128, 256);
      if (assignedMinorFragment == 0) {
        timesAssignedFragment0++;
      } else if (assignedMinorFragment == 1) {
        timesAssignedFragment1++;
      } else {
        timesAssignedFragment2++;
      }
    }
    assertEquals(numInvocations / 3, timesAssignedFragment0);
    assertEquals(numInvocations / 3, timesAssignedFragment1);
    assertEquals(numInvocations / 3, timesAssignedFragment2);
  }

  @Test
  public void testLoadDistributionYarn_fiveExecutors_threeWithEqualAffinities() {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(5);
    SplitHasher hasher = new SplitHasher(minorFragmentEndpoints);

    BlockLocations blockLocations1 = BlockLocations.newBuilder().setOffset(0).setSize(128)
      .addAllHosts(Arrays.asList("host0:9047", "host1:9047", "host2:9047"))
      .build();
    BlockLocations blockLocations2 = BlockLocations.newBuilder().setOffset(128).setSize(10)
      .addAllHosts(Arrays.asList("host3:9047", "host4:9047"))
      .build();
    BlockLocationsList blockLocationsList = BlockLocationsList.newBuilder()
      .addAllBlockLocations(Arrays.asList(blockLocations1, blockLocations2))
      .build();

    int timesAssignedFragment0 = 0;
    int timesAssignedFragment1 = 0;
    int timesAssignedFragment2 = 0;
    int numInvocations = 99;
    for (int i = 0; i < numInvocations; i++) {
      int assignedMinorFragment = hasher.getMinorFragmentIndex(null, blockLocationsList, 0, 256);
      if (assignedMinorFragment == 0) {
        timesAssignedFragment0++;
      } else if (assignedMinorFragment == 1) {
        timesAssignedFragment1++;
      } else {
        timesAssignedFragment2++;
      }
    }
    assertEquals(numInvocations / 3, timesAssignedFragment0);
    assertEquals(numInvocations / 3, timesAssignedFragment1);
    assertEquals(numInvocations / 3, timesAssignedFragment2);
  }

  private void verifySplitHasherWithShuffledPaths(int endPointCount, int splitCount) {
    final int offset = 0;

    // first run
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(endPointCount);
    SplitHasher splitHasher = new SplitHasher(minorFragmentEndpoints);
    List<String> paths = getSplitPaths(splitCount);
    Map<String, String> selectedHostsBeforeShuffle = new HashMap<>();
    for (int i = 0; i < splitCount; ++i) {
      int nodeIndex = splitHasher.getMinorFragmentIndexUsingRendezvousHash(Path.of(paths.get(i)), offset);
      selectedHostsBeforeShuffle.put(
        paths.get(i),
        String.format("%s:%d",
          minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
          minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort()));
    }

    // second run with shuffled paths
    splitHasher = new SplitHasher(minorFragmentEndpoints);
    Collections.shuffle(paths);
    for (int i = 0; i < splitCount; ++i) {
      int nodeIndex = splitHasher.getMinorFragmentIndexUsingRendezvousHash(Path.of(paths.get(i)), offset);
      String selectedNode = String.format("%s:%d",
        minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
        minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort());
      assertEquals(selectedNode, selectedHostsBeforeShuffle.get(paths.get(i)));
    }
  }

  private void verifySplitHasherWithDifferentEndPoints(int endPointCount, int splitCount) {
    final int offset = 0;

    // first run
    List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(endPointCount);
    SplitHasher splitHasher = new SplitHasher(minorFragmentEndpoints);
    List<String> paths = getSplitPaths(splitCount);
    Map<String, String> selectedHostsBeforeShuffle = new HashMap<>();
    for (int i = 0; i < splitCount; ++i) {
      int nodeIndex = splitHasher.getMinorFragmentIndexUsingRendezvousHash(Path.of(paths.get(i)), offset);
      selectedHostsBeforeShuffle.put(
        paths.get(i),
        String.format("%s:%d",
          minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
          minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort()));
    }

    // second run with shuffled paths and more endpoints
    // add 3 extra end points on first three nodes
    for (int i = 0; i < 3; ++i) {
      minorFragmentEndpoints.add(new MinorFragmentEndpoint(
        minorFragmentEndpoints.size() + 1,
        NodeEndpoint.newBuilder()
          .setAddress("host" + i)
          .setFabricPort(9047)
          .build()
      ));
    }
    splitHasher = new SplitHasher(minorFragmentEndpoints);
    Collections.shuffle(paths);

    // splits should go to same target node
    for (int i = 0; i < splitCount; ++i) {
      int nodeIndex = splitHasher.getMinorFragmentIndexUsingRendezvousHash(Path.of(paths.get(i)), offset);
      String selectedNode = String.format("%s:%d",
        minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
        minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort());
      assertEquals(selectedNode, selectedHostsBeforeShuffle.get(paths.get(i)));
    }
  }

  private List<String> getSplitPaths(int n) {
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      paths.add(String.format("split%d", i));
    }
    return paths;
  }

  private List<MinorFragmentEndpoint> getMinorFragmentEndpointList(int n) {
    List<MinorFragmentEndpoint> minorFragmentEndpoints = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      NodeEndpoint endpoint = NodeEndpoint.newBuilder()
        .setAddress(String.format("host%d", i))
        .setFabricPort(9047)
        .build();
      minorFragmentEndpoints.add(new MinorFragmentEndpoint(i, endpoint));
    }
    return minorFragmentEndpoints;
  }
}