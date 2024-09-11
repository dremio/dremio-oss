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

import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.google.common.base.Preconditions;

/**
 * Holder for the major and minor specific portions of the plan fragment. Has the capability to
 * release portions of the plan. Typically used once setup is complete
 */
public class PlanFragmentFullForExec {
  private final PlanFragmentMinor minor;
  private final FragmentHandle handle;
  private final NodeEndpoint foreman;
  private volatile PlanFragmentFull delegate;

  public PlanFragmentFullForExec(PlanFragmentFull fromPlan) {
    this.foreman = fromPlan.getMajor().getForeman();
    this.minor = fromPlan.getMinor();
    this.handle = fromPlan.getHandle();
    this.delegate = fromPlan;
  }

  public FragmentHandle getHandle() {
    return handle;
  }

  public PlanFragmentMinor getMinor() {
    return minor;
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

  public void releaseFullPlan() {
    Preconditions.checkState(delegate != null, "Already released major plan");
    delegate = null;
  }

  public PlanFragmentFull asPlanFragmentFull() {
    Preconditions.checkState(delegate != null, "Fragment plan no longer available");
    return delegate;
  }

  public NodeEndpoint getForeman() {
    return foreman;
  }
}
