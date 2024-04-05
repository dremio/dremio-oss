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

import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;

/** Holder for the major and minor specific portions of the plan fragment. */
public class PlanFragmentFull {
  private final PlanFragmentMajor major;
  private final PlanFragmentMinor minor;
  private FragmentHandle handle;

  public PlanFragmentFull(PlanFragmentMajor major, PlanFragmentMinor minor) {
    this.major = major;
    this.minor = minor;

    handle =
        FragmentHandle.newBuilder(major.getHandle())
            .setMinorFragmentId(minor.getMinorFragmentId())
            .build();
  }

  public PlanFragmentMajor getMajor() {
    return major;
  }

  public PlanFragmentMinor getMinor() {
    return minor;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  public int getMajorFragmentId() {
    return handle.getMajorFragmentId();
  }

  public int getMinorFragmentId() {
    return handle.getMinorFragmentId();
  }

  public NodeEndpoint getAssignment() {
    return minor.getAssignment();
  }

  public long getMemInitial() {
    return major.getMemInitial();
  }

  public long getMemMax() {
    return minor.getMemMax();
  }
}
