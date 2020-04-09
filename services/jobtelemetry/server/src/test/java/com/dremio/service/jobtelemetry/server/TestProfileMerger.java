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
package com.dremio.service.jobtelemetry.server;

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;

import org.junit.Test;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.FragmentState;

/**
 * Test for profile merge.
 */
public class TestProfileMerger {

  @Test
  public void testMergeWithOnlyPlanningProfile() {
    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      planningProfile.toBuilder()
        .setTotalFragments(0)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null, Stream.empty()));
  }

  @Test
  public void testMergeWithOnlyPlanningAndTailProfile() {
    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    final UserBitShared.QueryProfile tailProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .setTotalFragments(0)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, tailProfile, Stream.empty()));
  }

  @Test
  public void testMergeWithOnlyPlanningAndExecutorProfiles() {
    final CoordinationProtos.NodeEndpoint nodeEndPoint = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.666")
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint)
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .setTotalFragments(0)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null, Stream.of(executorQueryProfile)));
  }

  @Test
  public void testMergeWithOnlyThreeParts() {
    final CoordinationProtos.NodeEndpoint nodeEndPoint = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.666")
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .build();

    final UserBitShared.QueryProfile tailProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint)
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .setTotalFragments(0)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, tailProfile, Stream.of(executorQueryProfile)));
  }

  @Test
  public void testMergeWithMultipleExecutorProfiles() {
    final CoordinationProtos.NodeEndpoint nodeEndPoint1 = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.66")
      .build();
    final CoordinationProtos.NodeEndpoint nodeEndPoint2 = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.67")
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    // executor profile from node 1
    CoordExecRPC.NodePhaseStatus node1Phase0 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(0)
      .setMaxMemoryUsed(10)
      .build();

    CoordExecRPC.NodePhaseStatus node1Phase1 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(1)
      .setMaxMemoryUsed(11)
      .build();

    CoordExecRPC.FragmentStatus node1Frag0 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(0).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(116)
        .setState(FragmentState.FINISHED).build())
      .build();

    CoordExecRPC.FragmentStatus node1Frag1 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(1).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(117).build())
      .build();

    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile1 =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint1)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .addPhaseStatus(node1Phase0)
            .addPhaseStatus(node1Phase1)
            .build()
        )
        .addFragments(node1Frag0)
        .addFragments(node1Frag1)
        .build();

    // executor profile from node 2
    CoordExecRPC.NodePhaseStatus node2Phase0 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(0)
      .setMaxMemoryUsed(20)
      .build();

    CoordExecRPC.NodePhaseStatus node2Phase1 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(1)
      .setMaxMemoryUsed(21)
      .build();

    CoordExecRPC.FragmentStatus node2Frag0 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(0).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(216)
        .setState(FragmentState.FINISHED).build())
      .build();

    CoordExecRPC.FragmentStatus node2Frag1 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(1).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(217).build())
      .build();


    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile2 =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint2)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666667)
            .setTimeEnqueuedBeforeSubmitMs(3)
            .addPhaseStatus(node2Phase0)
            .addPhaseStatus(node2Phase1)
            .build()
        )
        .addFragments(node2Frag0)
        .addFragments(node2Frag1)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint1)
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint2)
            .setMaxMemoryUsed(666667)
            .setTimeEnqueuedBeforeSubmitMs(3)
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(0)
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint1)
                .setMaxMemoryUsed(node1Phase0.getMaxMemoryUsed())
                .build()
            )
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint2)
                .setMaxMemoryUsed(node2Phase0.getMaxMemoryUsed())
                .build()
            )
            .addMinorFragmentProfile(node1Frag0.getProfile())
            .addMinorFragmentProfile(node2Frag0.getProfile())
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(1)
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint1)
                .setMaxMemoryUsed(node1Phase1.getMaxMemoryUsed())
                .build()
            )
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint2)
                .setMaxMemoryUsed(node2Phase1.getMaxMemoryUsed())
                .build()
            )
            .addMinorFragmentProfile(node1Frag1.getProfile())
            .addMinorFragmentProfile(node2Frag1.getProfile())
            .build()
        )
        .setTotalFragments(4)
        .setFinishedFragments(2)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null,
        Stream.of(executorQueryProfile1, executorQueryProfile2)));
  }

  @Test
  public void testFragmentCountFromPlanningProfile() {
    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.COMPLETED)
        .setTotalFragments(5)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      planningProfile.toBuilder()
        .setTotalFragments(5)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null, Stream.empty()));
  }

  // phase profile includes only phase 0, fragment profiles include phase & phase 1.
  @Test
  public void testMergeWithInconsistentExecutorProfiles() {
    final CoordinationProtos.NodeEndpoint nodeEndPoint = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.666")
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    CoordExecRPC.NodePhaseStatus phase0 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(0)
      .setMaxMemoryUsed(20)
      .build();

    CoordExecRPC.FragmentStatus frag0 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(0).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(216)
        .setState(FragmentState.FINISHED).build())
      .build();

    CoordExecRPC.FragmentStatus frag1 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(1).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(216)
        .setState(FragmentState.FINISHED).build())
      .build();

    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .addPhaseStatus(phase0)
            .build()
        )
        .addFragments(frag0)
        .addFragments(frag1)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint)
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(0)
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint)
                .setMaxMemoryUsed(phase0.getMaxMemoryUsed())
                .build()
            )
            .addMinorFragmentProfile(frag0.getProfile())
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(1)
            .addMinorFragmentProfile(frag1.getProfile())
            .build()
        )
        .setTotalFragments(2)
        .setFinishedFragments(2)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null, Stream.of(executorQueryProfile)));
  }

  // phase profile includes phase 0 and phase 1, fragment profiles include phase 0
  @Test
  public void testMergeWithInconsistentExecutorProfiles2() {
    final CoordinationProtos.NodeEndpoint nodeEndPoint = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .setAddress("190.190.0.666")
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .build();

    CoordExecRPC.NodePhaseStatus phase0 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(0)
      .setMaxMemoryUsed(20)
      .build();

    CoordExecRPC.NodePhaseStatus phase1 = CoordExecRPC.NodePhaseStatus.newBuilder()
      .setMajorFragmentId(1)
      .setMaxMemoryUsed(30)
      .build();

    CoordExecRPC.FragmentStatus frag0 = CoordExecRPC.FragmentStatus.newBuilder()
      .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(0).build())
      .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(216)
        .setState(FragmentState.FINISHED).build())
      .build();

    final CoordExecRPC.ExecutorQueryProfile executorQueryProfile =
      CoordExecRPC.ExecutorQueryProfile.newBuilder()
        .setEndpoint(nodeEndPoint)
        .setNodeStatus(
          CoordExecRPC.NodeQueryStatus.newBuilder()
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .addPhaseStatus(phase0)
            .addPhaseStatus(phase1)
            .build()
        )
        .addFragments(frag0)
        .build();

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .addNodeProfile(
          UserBitShared.NodeQueryProfile.newBuilder()
            .setEndpoint(nodeEndPoint)
            .setMaxMemoryUsed(666666)
            .setTimeEnqueuedBeforeSubmitMs(2)
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(0)
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint)
                .setMaxMemoryUsed(phase0.getMaxMemoryUsed())
                .build()
            )
            .addMinorFragmentProfile(frag0.getProfile())
            .build()
        )
        .addFragmentProfile(
          UserBitShared.MajorFragmentProfile.newBuilder()
            .setMajorFragmentId(1)
            .addNodePhaseProfile(
              UserBitShared.NodePhaseProfile.newBuilder()
                .setEndpoint(nodeEndPoint)
                .setMaxMemoryUsed(phase1.getMaxMemoryUsed())
                .build()
            )
            .build()
        )
        .setTotalFragments(1)
        .setFinishedFragments(1)
        .build();

    assertEquals(expectedMergedProfile,
      ProfileMerger.merge(planningProfile, null, Stream.of(executorQueryProfile)));
  }
}
