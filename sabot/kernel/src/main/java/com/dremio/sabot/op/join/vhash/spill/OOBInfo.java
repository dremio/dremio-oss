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
package com.dremio.sabot.op.join.vhash.spill;

import java.util.List;

import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.exec.rpc.TunnelProvider;

/**
 * Relays info required to send an OOB message from {@link VectorizedSpillingHashJoinOperator} to MemoryPartition, since spilling
 * of VectorizedSpillingHashJoinOperator is performed from MemoryPartition.
 */
public class OOBInfo {

  private List<CoordExecRPC.FragmentAssignment> assignments;
  private QueryId queryId;
  private int majorFragmentId;
  private int operatorId;
  private int minorFragmentId;
  private EndpointsIndex endpointsIndex;
  private TunnelProvider tunnelProvider;
  private boolean oobSpillEnabled;
  private String message_type;

  public OOBInfo(final List<CoordExecRPC.FragmentAssignment> assignments, final QueryId queryId, final int majorFragmentId,
                 final int operatorId, final int minorFragmentId, final EndpointsIndex endpointsIndex,
                 final TunnelProvider tunnelProvider, final boolean oobSpillEnabled, final String message_type) {
    this.assignments = assignments;
    this.queryId = queryId;
    this.majorFragmentId = majorFragmentId;
    this.operatorId = operatorId;
    this.minorFragmentId = minorFragmentId;
    this.endpointsIndex = endpointsIndex;
    this.tunnelProvider = tunnelProvider;
    this.oobSpillEnabled = oobSpillEnabled;
    this.message_type = message_type;
  }

  public List<CoordExecRPC.FragmentAssignment> getAssignments() {
    return assignments;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public int getMinorFragmentId() {
    return minorFragmentId;
  }

  public EndpointsIndex getEndpointsIndex() {
    return endpointsIndex;
  }

  public TunnelProvider getTunnelProvider() {
    return tunnelProvider;
  }

  public boolean isOobSpillEnabled() {
    return oobSpillEnabled;
  }

  public String getMessage_type() {
    return message_type;
  }
}
