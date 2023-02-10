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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodePhaseProfile;
import com.dremio.exec.proto.UserBitShared.NodeQueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.google.common.base.Preconditions;

/**
 * Merger for all portions of a query profile.
 */
final class ProfileMerger {
  private final QueryProfile planningProfile;
  private final QueryProfile tailProfile;
  private final List<ExecutorQueryProfile> executorQueryProfiles;
  private final List<NodeQueryProfile> nodeProfiles = new ArrayList<>();
  private final List<MajorFragmentProfile.Builder> phaseProfiles;
  private int totalFragments;
  private int finishedFragments;

  private ProfileMerger(QueryProfile planningProfile, QueryProfile tailProfile,
      Stream<ExecutorQueryProfile> executorProfiles) {
    this.planningProfile = planningProfile;
    this.tailProfile = tailProfile;
    this.executorQueryProfiles = executorProfiles.collect(Collectors.toList());
    this.phaseProfiles = createEmptyPhaseProfiles();
  }

  private List<MajorFragmentProfile.Builder> createEmptyPhaseProfiles() {
    Map<Integer, Integer> phaseWeights = executorQueryProfiles.stream()
      .flatMap(executorProfile -> executorProfile.getNodeStatus().getPhaseStatusList().stream())
      .collect(Collectors.toMap(NodePhaseStatus::getMajorFragmentId,
        (v) -> (v.hasPhaseWeight() ? v.getPhaseWeight() : -1), (v1, v2) -> v1 >= 0 ? v1 : v2));

    // find the max major fragment id.
    // this should work even if the phase list, and the fragment list are inconsistent.
    int maxPhaseId = phaseWeights.keySet().stream().mapToInt(x -> x).max().orElse(-1);

    int maxFragmentPhaseId = executorQueryProfiles.stream()
      .flatMap(executorProfile -> executorProfile.getFragmentsList().stream())
      .mapToInt(frag -> frag.getHandle().getMajorFragmentId())
      .max()
      .orElse(-1);

    maxPhaseId = Integer.max(maxPhaseId, maxFragmentPhaseId);

    // create empty profiles for all phases.
    List<MajorFragmentProfile.Builder> phaseList = new ArrayList<>();
    for (int i = 0; i <= maxPhaseId; i++) {
      final int phaseWeight = phaseWeights.getOrDefault(i, -1);
      MajorFragmentProfile.Builder mfb = MajorFragmentProfile
        .newBuilder()
        .setMajorFragmentId(i);
      if (phaseWeight > 0) {
        mfb = mfb.setPhaseWeight(phaseWeight);
      }
      phaseList.add(mfb);
    }
    return phaseList;
  }

  static QueryProfile merge(QueryProfile planningProfile, QueryProfile tailProfile,
    Stream<ExecutorQueryProfile> executorProfiles) {

    return new ProfileMerger(planningProfile, tailProfile, executorProfiles).merge();
  }

  private QueryProfile merge() {
    QueryProfile.Builder builder = QueryProfile.newBuilder();
    int maxTotalFragments = 0;

    // at least one of the two should be present.
    Preconditions.checkState(planningProfile != null || tailProfile != null);

    // fill up details from tailProfile. Only if it is not present, use the
    // planning profile.
    if (tailProfile != null) {
      builder.mergeFrom(tailProfile);
      maxTotalFragments = Math.max(maxTotalFragments, tailProfile.getTotalFragments());
    } else {
      builder.mergeFrom(planningProfile);
      maxTotalFragments = Math.max(maxTotalFragments, planningProfile.getTotalFragments());
    }

    // fill up details from the executor profiles.
    executorQueryProfiles.forEach(this::processExecutorProfile);
    maxTotalFragments = Math.max(maxTotalFragments, totalFragments);

    return builder
      .addAllNodeProfile(nodeProfiles)
      .addAllFragmentProfile(
        phaseProfiles.stream()
          .map(MajorFragmentProfile.Builder::build)
          .collect(Collectors.toList())
      )
      .setTotalFragments(maxTotalFragments)
      .setFinishedFragments(finishedFragments)
      .build();
  }

  private void processExecutorProfile(ExecutorQueryProfile executorProfile) {
    updateNodeProfile(executorProfile);
    updatePhaseProfiles(executorProfile);
  }

  private void updateNodeProfile(ExecutorQueryProfile executorProfile) {
    final NodeQueryStatus status = executorProfile.getNodeStatus();

    nodeProfiles.add(
      NodeQueryProfile.newBuilder()
        .setEndpoint(executorProfile.getEndpoint())
        .setMaxMemoryUsed(status.getMaxMemoryUsed())
        .setTimeEnqueuedBeforeSubmitMs(status.getTimeEnqueuedBeforeSubmitMs())
        .setNumberOfCores(status.getNumberOfCores())
        .build()
    );
  }

  private void updatePhaseProfiles(ExecutorQueryProfile executorProfile) {
    // update per-node status for each phase.
    for (NodePhaseStatus nodePhaseStatus : executorProfile.getNodeStatus().getPhaseStatusList()) {
      int phaseId = nodePhaseStatus.getMajorFragmentId();

      NodePhaseProfile nodePhaseProfile = NodePhaseProfile.newBuilder()
        .setEndpoint(executorProfile.getEndpoint())
        .setMaxMemoryUsed(nodePhaseStatus.getMaxMemoryUsed())
        .build();
      phaseProfiles.get(phaseId).addNodePhaseProfile(nodePhaseProfile);
    }

    // update fragment status for each phase.
    for (FragmentStatus fragmentStatus : executorProfile.getFragmentsList()) {
      int phaseId = fragmentStatus.getHandle().getMajorFragmentId();
      phaseProfiles.get(phaseId).addMinorFragmentProfile(fragmentStatus.getProfile());

      ++totalFragments;
      if (isTerminal(fragmentStatus.getProfile().getState())) {
        ++finishedFragments;
      }
    }
  }

  private boolean isTerminal(FragmentState state) {
    return (state == FragmentState.FINISHED
      || state == FragmentState.FAILED
      || state == FragmentState.CANCELLED);
  }
}
