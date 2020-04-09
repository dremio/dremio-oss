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
package com.dremio.sabot.exec.fragment;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.FragmentState;
import com.dremio.exec.proto.UserBitShared.MinorFragmentProfile;
import com.dremio.exec.proto.UserBitShared.OperatorProfile;
import com.dremio.sabot.exec.MaestroProxy;
import com.dremio.sabot.exec.context.FragmentStats;

/**
 * The status reporter is responsible for receiving changes in fragment state and propagating the status back to the
 * AttemptManager through a control tunnel.
 */
class FragmentStatusReporter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStatusReporter.class);

  private final FragmentHandle handle;
  private final FragmentStats stats;
  private final MaestroProxy maestroProxy;
  private final BufferAllocator fragmentAllocator;
  private MinorFragmentProfile prevMinorFragmentProfile;

  public FragmentStatusReporter(
      final FragmentHandle handle,
      final FragmentStats stats,
      final MaestroProxy maestroProxy,
      final BufferAllocator fragmentAllocator) {
    this.handle = handle;
    this.stats = stats;
    this.maestroProxy = maestroProxy;
    this.fragmentAllocator = fragmentAllocator;
  }

  /**
   * Returns a {@link FragmentStatus} with the given state. {@link FragmentStatus} has additional information like
   * metrics, etc. that is gathered from the {@link FragmentContext}.
   *
   * @param state the state to include in the status
   * @return the status
   */
  public FragmentStatus getStatus(final FragmentState state) {
    return getStatus(state, null);
  }

  private FragmentStatus getStatus(final FragmentState state, final UserException ex) {
    final FragmentStatus.Builder status = FragmentStatus.newBuilder();
    final MinorFragmentProfile.Builder b = MinorFragmentProfile.newBuilder();
    stats.addMetricsToStatus(b);
    b.setState(state);
    if (ex != null) {
      b.setError(ex.getOrCreatePBError(true));
    }
    status.setHandle(handle);
    b.setMemoryUsed(fragmentAllocator.getAllocatedMemory());
    b.setMinorFragmentId(handle.getMinorFragmentId());

    final long time = System.currentTimeMillis();
    b.setLastUpdate(time);
    if (prevMinorFragmentProfile == null || madeProgress(prevMinorFragmentProfile, b.build())) {
      b.setLastProgress(time);
    } else {
      b.setLastProgress(prevMinorFragmentProfile.getLastProgress());
    }

    status.setProfile(b);
    prevMinorFragmentProfile = status.getProfile();
    return status.build();
  }

  /**
   * Reports the state change to the AttemptManager. The state is wrapped in a {@link FragmentStatus} that has additional
   * information like metrics, etc. This additional information is gathered from the {@link FragmentContext}.
   * NOTE: Use {@link #fail} to report state change to {@link FragmentState#FAILED}.
   *
   * @param newState the new state
   */
  public void stateChanged(final FragmentState newState) {
    final FragmentStatus status = getStatus(newState, null);
    logger.info("{}: State to report: {}", QueryIdHelper.getQueryIdentifier(handle), newState);
    switch (newState) {
    case AWAITING_ALLOCATION:
    case CANCELLATION_REQUESTED:
    case CANCELLED:
    case FINISHED:
    case RUNNING:
      notifyStatusChanged(status);
      break;
    case SENDING:
      // no op.
      break;
    case FAILED:
      // shouldn't get here since fail() should be called.
    default:
      throw new IllegalStateException(String.format("Received state changed event for unexpected state of %s.", newState));
    }
  }

  /**
   * {@link FragmentStatus} with the {@link FragmentState#FAILED} state is reported to the AttemptManager. The
   * {@link FragmentStatus} has additional information like metrics, etc. that is gathered from the
   * {@link FragmentContext}.
   *
   * @param ex the exception related to the failure
   */
  public void fail(final UserException ex) {
    final FragmentStatus status = getStatus(FragmentState.FAILED, ex);
    notifyStatusChanged(status);
  }

  private void notifyStatusChanged(final FragmentStatus status) {
    maestroProxy.fragmentStatusChanged(status);
  }

  private boolean madeProgress(final MinorFragmentProfile prev, final MinorFragmentProfile cur) {
    if (prev.getState() != cur.getState()) {
      return true;
    }

    if (prev.getOperatorProfileCount() != cur.getOperatorProfileCount()) {
      return true;
    }

    for (int i = 0; i < cur.getOperatorProfileCount(); i++) {
      if (madeProgress(prev.getOperatorProfile(i), cur.getOperatorProfile(i))) {
        return true;
      }
    }

    return false;
  }

  private boolean madeProgress(final OperatorProfile prev, final OperatorProfile cur) {
    return prev.getInputProfileCount() != cur.getInputProfileCount()
      || !prev.getInputProfileList().equals(cur.getInputProfileList())
      || prev.getMetricCount() != cur.getMetricCount()
      || !prev.getMetricList().equals(cur.getMetricList());
  }
}
