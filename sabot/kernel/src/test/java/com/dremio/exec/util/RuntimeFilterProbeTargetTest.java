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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.physical.config.RuntimeFilterProbeTarget;
import com.dremio.exec.planner.physical.filter.RuntimeFilterInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Test;

/** Tests for {@link RuntimeFilterProbeTarget} */
public class RuntimeFilterProbeTargetTest {

  @Test
  public void testMultipleProbeTargets() {
    int majorFragment1 = 1;
    int opId1 = 101;

    int majorFragment2 = 2;
    int opId2 = 202;

    RuntimeFilterInfo filterInfo =
        new RuntimeFilterInfo.Builder()
            .isBroadcastJoin(false)
            .setRuntimeFilterProbeTargets(
                ImmutableList.of(
                    new RuntimeFilterProbeTarget.Builder(majorFragment1, opId1)
                        .addNonPartitionKey("np_build_field1_target1", "np_probe_field1_target1")
                        .addNonPartitionKey("np_build_field2_target1", "np_probe_field2_target1")
                        .addPartitionKey("p_build_field1_target1", "p_probe_field1_target1")
                        .addPartitionKey("p_build_field2_target1", "p_probe_field2_target1")
                        .build(),
                    new RuntimeFilterProbeTarget.Builder(majorFragment2, opId2)
                        .addNonPartitionKey("np_build_field1_target2", "np_probe_field1_target2")
                        .addNonPartitionKey("np_build_field2_target2", "np_probe_field2_target2")
                        .addPartitionKey("p_build_field1_target2", "p_probe_field1_target2")
                        .addPartitionKey("p_build_field2_target2", "p_probe_field2_target2")
                        .build()))
            .build();

    List<RuntimeFilterProbeTarget> probeTargets = filterInfo.getRuntimeFilterProbeTargets();
    assertEquals(2, probeTargets.size());

    RuntimeFilterProbeTarget target1 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment1, opId1))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(
        Lists.newArrayList("p_build_field1_target1", "p_build_field2_target1"),
        target1.getPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("p_probe_field1_target1", "p_probe_field2_target1"),
        target1.getPartitionProbeTableKeys());

    assertEquals(
        Lists.newArrayList("np_build_field1_target1", "np_build_field2_target1"),
        target1.getNonPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("np_probe_field1_target1", "np_probe_field2_target1"),
        target1.getNonPartitionProbeTableKeys());

    RuntimeFilterProbeTarget target2 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment2, opId2))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(
        Lists.newArrayList("p_build_field1_target2", "p_build_field2_target2"),
        target2.getPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("p_probe_field1_target2", "p_probe_field2_target2"),
        target2.getPartitionProbeTableKeys());

    assertEquals(
        Lists.newArrayList("np_build_field1_target2", "np_build_field2_target2"),
        target2.getNonPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("np_probe_field1_target2", "np_probe_field2_target2"),
        target2.getNonPartitionProbeTableKeys());
  }

  @Test
  public void testProbeTargetMultipleBuildKeyMappings() {
    int majorFragment1 = 1;
    int opId1 = 101;

    int majorFragment2 = 2;
    int opId2 = 202;

    /*
     * build_field1 is mapped against partition and non-partition columns in both probe targets.
     */
    RuntimeFilterInfo filterInfo =
        new RuntimeFilterInfo.Builder()
            .isBroadcastJoin(false)
            .setRuntimeFilterProbeTargets(
                ImmutableList.of(
                    new RuntimeFilterProbeTarget.Builder(majorFragment1, opId1)
                        .addNonPartitionKey("build_field1", "np_probe_field1_target1")
                        .addPartitionKey("build_field1", "p_probe_field1_target1")
                        .build(),
                    new RuntimeFilterProbeTarget.Builder(majorFragment2, opId2)
                        .addNonPartitionKey("build_field1", "np_probe_field1_target2")
                        .addPartitionKey("build_field1", "p_probe_field1_target2")
                        .build()))
            .build();

    List<RuntimeFilterProbeTarget> probeTargets = filterInfo.getRuntimeFilterProbeTargets();
    assertEquals(2, probeTargets.size());
    RuntimeFilterProbeTarget target1 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment1, opId1))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(Lists.newArrayList("build_field1"), target1.getPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("p_probe_field1_target1"), target1.getPartitionProbeTableKeys());

    assertEquals(Lists.newArrayList("build_field1"), target1.getNonPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("np_probe_field1_target1"), target1.getNonPartitionProbeTableKeys());

    RuntimeFilterProbeTarget target2 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment2, opId2))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(Lists.newArrayList("build_field1"), target2.getPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("p_probe_field1_target2"), target2.getPartitionProbeTableKeys());

    assertEquals(Lists.newArrayList("build_field1"), target2.getNonPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("np_probe_field1_target2"), target2.getNonPartitionProbeTableKeys());
  }

  @Test
  public void testProbeTargetMultipleProbeKeyMappings() {
    int majorFragment1 = 1;
    int opId1 = 101;

    int majorFragment2 = 2;
    int opId2 = 202;

    /*
     * build_field1 is mapped against partition and non-partition columns in both probe targets.
     */

    RuntimeFilterInfo filterInfo =
        new RuntimeFilterInfo.Builder()
            .isBroadcastJoin(false)
            .setRuntimeFilterProbeTargets(
                ImmutableList.of(
                    new RuntimeFilterProbeTarget.Builder(majorFragment1, opId1)
                        .addPartitionKey("p_build_field1_target1", "probe_field")
                        .addNonPartitionKey("np_build_field1_target1", "probe_field")
                        .build(),
                    new RuntimeFilterProbeTarget.Builder(majorFragment2, opId2)
                        .addPartitionKey("p_build_field1_target2", "probe_field")
                        .addNonPartitionKey("np_build_field1_target2", "probe_field")
                        .build()))
            .build();

    List<RuntimeFilterProbeTarget> probeTargets = filterInfo.getRuntimeFilterProbeTargets();
    assertEquals(2, probeTargets.size());
    RuntimeFilterProbeTarget target1 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment1, opId1))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(
        Lists.newArrayList("p_build_field1_target1"), target1.getPartitionBuildTableKeys());
    assertEquals(Lists.newArrayList("probe_field"), target1.getPartitionProbeTableKeys());

    assertEquals(
        Lists.newArrayList("np_build_field1_target1"), target1.getNonPartitionBuildTableKeys());
    assertEquals(Lists.newArrayList("probe_field"), target1.getNonPartitionProbeTableKeys());

    RuntimeFilterProbeTarget target2 =
        probeTargets.stream()
            .filter(t -> t.isSameProbeCoordinate(majorFragment2, opId2))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Probe target not found"));
    assertEquals(
        Lists.newArrayList("p_build_field1_target2"), target2.getPartitionBuildTableKeys());
    assertEquals(Lists.newArrayList("probe_field"), target2.getPartitionProbeTableKeys());

    assertEquals(
        Lists.newArrayList("np_build_field1_target2"), target2.getNonPartitionBuildTableKeys());
    assertEquals(Lists.newArrayList("probe_field"), target2.getNonPartitionProbeTableKeys());
  }

  @Test
  public void testSinglePartitionCol() {

    RuntimeFilterInfo filterInfo =
        new RuntimeFilterInfo.Builder()
            .isBroadcastJoin(false)
            .setRuntimeFilterProbeTargets(
                ImmutableList.of(
                    new RuntimeFilterProbeTarget.Builder(1, 101)
                        .addPartitionKey("buildField", "probe_field")
                        .build()))
            .build();

    List<RuntimeFilterProbeTarget> probeTargets = filterInfo.getRuntimeFilterProbeTargets();
    assertEquals(1, probeTargets.size());
    assertEquals(
        Lists.newArrayList("buildField"), probeTargets.get(0).getPartitionBuildTableKeys());
    assertEquals(
        Lists.newArrayList("probe_field"), probeTargets.get(0).getPartitionProbeTableKeys());
    assertTrue(probeTargets.get(0).getNonPartitionBuildTableKeys().isEmpty());
    assertTrue(probeTargets.get(0).getNonPartitionProbeTableKeys().isEmpty());
  }
}
