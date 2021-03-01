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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.physical.config.MinorFragmentEndpoint;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.io.file.Path;

/**
 * Tests for RendezvousSplitHasher
 */
public class RendezvousSplitHasherTest {

    private List<String> getSplitPaths(int n) {
        List<String> paths = new ArrayList<>();
        for (int i=0; i<n; ++i) {
            paths.add(String.format("split%d", i));
        }
        return paths;
    }

    private List<MinorFragmentEndpoint> getMinorFragmentEndpointList(int n) {
        List<MinorFragmentEndpoint> minorFragmentEndpoints = new ArrayList<>();
        for (int i=0; i<n; ++i) {
            minorFragmentEndpoints.add(new MinorFragmentEndpoint(i,
                    NodeEndpoint.newBuilder()
                            .setAddress(String.format("host%d", i))
                            .setFabricPort(9047)
                            .build()));
        }
        return minorFragmentEndpoints;
    }

    @Test
    public void testSplitHasherWithShuffledPaths() {
        verifySplitHasherWithShuffledPaths(1, 100);
        verifySplitHasherWithShuffledPaths(100, 1);
        verifySplitHasherWithShuffledPaths(10, 100);
        verifySplitHasherWithShuffledPaths(100, 1000);
    }

    private void verifySplitHasherWithShuffledPaths(int endPointCount, int splitCount) {
        final int offset = 0;

        // first run
        List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(endPointCount);
        RendezvousSplitHasher splitHasher = new RendezvousSplitHasher(minorFragmentEndpoints);
        List<String> paths = getSplitPaths(splitCount);
        Map<String, String> selectedHostsBeforeShuffle = new HashMap<>();
        for (int i=0; i<splitCount; ++i) {
            int nodeIndex = splitHasher.getNodeIndex(Path.of(paths.get(i)), offset);
            selectedHostsBeforeShuffle.put(
                    paths.get(i),
                    String.format("%s:%d",
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort()));
        }

        // second run with shuffled paths
        splitHasher = new RendezvousSplitHasher(minorFragmentEndpoints);
        Collections.shuffle(paths);
        for (int i=0; i<splitCount; ++i) {
            int nodeIndex = splitHasher.getNodeIndex(Path.of(paths.get(i)), offset);
            String selectedNode = String.format("%s:%d",
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort());
            Assert.assertEquals(selectedNode,selectedHostsBeforeShuffle.get(paths.get(i)));
        }
    }

    @Test
    public void testSplitHasherWithDifferentEndPoints() {
        verifySplitHasherWithDifferentEndPoints(3, 100);
        verifySplitHasherWithDifferentEndPoints(100, 1);
        verifySplitHasherWithDifferentEndPoints(10, 100);
        verifySplitHasherWithDifferentEndPoints(100, 1000);
    }

    private void verifySplitHasherWithDifferentEndPoints(int endPointCount, int splitCount) {
        final int offset = 0;

        // first run
        List<MinorFragmentEndpoint> minorFragmentEndpoints = getMinorFragmentEndpointList(endPointCount);
        RendezvousSplitHasher splitHasher = new RendezvousSplitHasher(minorFragmentEndpoints);
        List<String> paths = getSplitPaths(splitCount);
        Map<String, String> selectedHostsBeforeShuffle = new HashMap<>();
        for (int i=0; i<splitCount; ++i) {
            int nodeIndex = splitHasher.getNodeIndex(Path.of(paths.get(i)), offset);
            selectedHostsBeforeShuffle.put(
                    paths.get(i),
                    String.format("%s:%d",
                            minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
                            minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort()));
        }

        // second run with shuffled paths and more endpoints
        // add 3 extra end points on first three nodes
        for (int i=0; i<3; ++i) {
            minorFragmentEndpoints.add(new MinorFragmentEndpoint(
                    minorFragmentEndpoints.size() + 1,
                    NodeEndpoint.newBuilder()
                            .setAddress("host" + i)
                            .setFabricPort(9047)
                            .build()
            ));
        }
        splitHasher = new RendezvousSplitHasher(minorFragmentEndpoints);
        Collections.shuffle(paths);

        // splits should go to same target node
        for (int i=0; i<splitCount; ++i) {
            int nodeIndex = splitHasher.getNodeIndex(Path.of(paths.get(i)), offset);
            String selectedNode = String.format("%s:%d",
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getAddress(),
                    minorFragmentEndpoints.get(nodeIndex).getEndpoint().getFabricPort());
            Assert.assertEquals(selectedNode,selectedHostsBeforeShuffle.get(paths.get(i)));
        }
    }
}