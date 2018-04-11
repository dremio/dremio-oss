/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.server.admin.profile;

import java.util.Comparator;

import org.apache.commons.lang3.tuple.Pair;

import com.dremio.exec.proto.UserBitShared.MajorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.NodePhaseProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;

/**
 * Collection of comparators for comparing data in major fragment profiles, minor fragment profiles and
 * operator profiles.
 */
interface Comparators {

  Comparator<MajorFragmentProfile> majorId = new Comparator<MajorFragmentProfile>() {
    public int compare(final MajorFragmentProfile o1, final MajorFragmentProfile o2) {
      return Long.compare(o1.getMajorFragmentId(), o2.getMajorFragmentId());
    }
  };

  Comparator<MinorFragmentProfile> minorId = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getMinorFragmentId(), o2.getMinorFragmentId());
    }
  };

  Comparator<MinorFragmentProfile> startTime = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getStartTime(), o2.getStartTime());
    }
  };

  Comparator<MinorFragmentProfile> lastUpdate = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getLastUpdate(), o2.getLastUpdate());
    }
  };

  Comparator<MinorFragmentProfile> lastProgress = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getLastProgress(), o2.getLastProgress());
    }
  };

  Comparator<MinorFragmentProfile> endTime = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getEndTime(), o2.getEndTime());
    }
  };

  Comparator<MinorFragmentProfile> fragmentPeakMemory = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getMaxMemoryUsed(), o2.getMaxMemoryUsed());
    }
  };

  Comparator<MinorFragmentProfile> runTime = new Comparator<MinorFragmentProfile>() {
    public int compare(final MinorFragmentProfile o1, final MinorFragmentProfile o2) {
      return Long.compare(o1.getEndTime() - o1.getStartTime(), o2.getEndTime() - o2.getStartTime());
    }
  };

  Comparator<OperatorProfile> operatorId = new Comparator<OperatorProfile>() {
    public int compare(final OperatorProfile o1, final OperatorProfile o2) {
      return Long.compare(o1.getOperatorId(), o2.getOperatorId());
    }
  };

  Comparator<Pair<OperatorProfile, Integer>> setupTime = new Comparator<Pair<OperatorProfile, Integer>>() {
    public int compare(final Pair<OperatorProfile, Integer> o1, final Pair<OperatorProfile, Integer> o2) {
      return Long.compare(o1.getLeft().getSetupNanos(), o2.getLeft().getSetupNanos());
    }
  };

  Comparator<Pair<OperatorProfile, Integer>> processTime = new Comparator<Pair<OperatorProfile, Integer>>() {
    public int compare(final Pair<OperatorProfile, Integer> o1, final Pair<OperatorProfile, Integer> o2) {
      return Long.compare(o1.getLeft().getProcessNanos(), o2.getLeft().getProcessNanos());
    }
  };

  Comparator<Pair<OperatorProfile, Integer>> waitTime = new Comparator<Pair<OperatorProfile, Integer>>() {
    public int compare(final Pair<OperatorProfile, Integer> o1, final Pair<OperatorProfile, Integer> o2) {
      return Long.compare(o1.getLeft().getWaitNanos(), o2.getLeft().getWaitNanos());
    }
  };

  Comparator<Pair<OperatorProfile, Integer>> operatorPeakMemory = new Comparator<Pair<OperatorProfile, Integer>>() {
    public int compare(final Pair<OperatorProfile, Integer> o1, final Pair<OperatorProfile, Integer> o2) {
      return Long.compare(o1.getLeft().getPeakLocalMemoryAllocated(), o2.getLeft().getPeakLocalMemoryAllocated());
    }
  };

  Comparator<NodePhaseProfile> nodeAddress = new Comparator<NodePhaseProfile>() {
    @Override
    public int compare(NodePhaseProfile o1, NodePhaseProfile o2) {
      return o1.getEndpoint().getAddress().compareTo(o2.getEndpoint().getAddress());
    }
  };
}
