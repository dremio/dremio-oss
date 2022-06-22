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
package com.dremio.exec.store.common;

import static com.dremio.exec.store.common.HostAffinityComputer.computeSortedAffinitiesForSplit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocations;
import com.google.common.net.HostAndPort;

/**
 * Test for {@link HostAffinityComputer}
 */
public class HostAffinityComputerTest {
  @Test
  public void testEmptyBlockLocations() {
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(0, 0, Collections.emptyList());
    assertTrue(affinities.isEmpty());
  }

  @Test
  public void testNoOverlap_splitSmallerThanBlockAndOnTheLeft() {
    int splitStart = 0;
    int splitSize = 200;

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(splitSize * 2).setSize(splitSize * 2).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertTrue(affinities.isEmpty());
  }

  @Test
  public void testNoOverlap_splitSmallerThanBlockAndOnTheRight() {
    int splitStart = 400;
    int splitSize = 600;

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(400).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertTrue(affinities.isEmpty());
  }

  @Test
  public void testNoOverlap_splitLargerThanBlockAndOnTheLeft() {
    int splitStart = 0;
    int splitSize = 400;

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(400).setSize(200).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertTrue(affinities.isEmpty());
  }

  @Test
  public void testNoOverlap_splitLargerThanBlockAndOnTheRight() {
    int splitStart = 400;
    int splitSize = 800;

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(200).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertTrue(affinities.isEmpty());
  }

  @Test
  public void testFullOverlap_splitFitsWithinBlock() {
    int splitStart = 200;
    int splitSize = 400;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(600).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(1.0, affinities.get(0).getAffinity(), 0.0001);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(1.0, affinities.get(1).getAffinity(), 0.0001);
  }

  @Test
  public void testFullOverlap_splitExactlyOverlapsWithBlock() {
    int splitStart = 200;
    int splitSize = 400;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(splitStart).setSize(splitSize).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(1.0, affinities.get(0).getAffinity(), 0.0001);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(1.0, affinities.get(1).getAffinity(), 0.0001);
  }

  @Test
  public void testFullOverlap_blockFitsWithinSplit() {
    int splitStart = 0;
    int splitSize = 600;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(200).setSize(200).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(0.33, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(0.33, affinities.get(1).getAffinity(), 0.01);
  }

  @Test
  public void testPartialOverlap_splitSmallerThanBlockAndOnTheLeft() {
    int splitStart = 0;
    int splitSize = 200;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(100).setSize(splitSize * 3).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(0.5, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(0.5, affinities.get(1).getAffinity(), 0.01);
  }

  @Test
  public void testPartialOverlap_splitSmallerThanBlockAndOnTheRight() {
    int splitStart = 200;
    int splitSize = 200;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(300).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(0.5, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(0.5, affinities.get(1).getAffinity(), 0.01);
  }

  @Test
  public void testPartialOverlap_splitLargerThanBlockAndOnTheLeft() {
    int splitStart = 0;
    int splitSize = 300;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(200).setSize(splitSize / 2).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(0.33, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(0.33, affinities.get(1).getAffinity(), 0.01);
  }

  @Test
  public void testPartialOverlap_splitLargerThanBlockAndOnTheRight() {
    int splitStart = 200;
    int splitSize = 400;
    String host1 = "host1:1234";
    String host2 = "host2:2345";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(300).addAllHosts(Arrays.asList(host1, host2)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    assertEquals(2, affinities.size());
    assertEquals(HostAndPort.fromString(host1), affinities.get(0).getHostAndPort());
    assertEquals(0.25, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host2), affinities.get(1).getHostAndPort());
    assertEquals(0.25, affinities.get(1).getAffinity(), 0.01);
  }

  @Test
  public void testAffinitiesInDecreasingOrder_splitOverlapsWithMultipleBlocks() {
    int splitStart = 100;
    int splitSize = 500;
    String host1 = "host1:1234";
    String host2 = "host2:2345";
    String host3 = "host3:3456";

    List<BlockLocations> blockLocations = new ArrayList<>();
    blockLocations.add(BlockLocations.newBuilder().setOffset(0).setSize(200).addAllHosts(Arrays.asList(host1, host2)).build());
    blockLocations.add(BlockLocations.newBuilder().setOffset(400).setSize(400).addAllHosts(Arrays.asList(host2, host3)).build());
    List<EndpointsAffinity> affinities = computeSortedAffinitiesForSplit(splitStart, splitSize, blockLocations);
    System.out.println(affinities);
    assertEquals(3, affinities.size());
    assertEquals(HostAndPort.fromString(host2), affinities.get(0).getHostAndPort());
    assertEquals(0.6, affinities.get(0).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host3), affinities.get(1).getHostAndPort());
    assertEquals(0.4, affinities.get(1).getAffinity(), 0.01);

    assertEquals(HostAndPort.fromString(host1), affinities.get(2).getHostAndPort());
    assertEquals(0.2, affinities.get(2).getAffinity(), 0.01);
  }
}
