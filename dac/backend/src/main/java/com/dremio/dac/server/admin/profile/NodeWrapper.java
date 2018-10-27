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

import com.dremio.exec.proto.UserBitShared.NodeQueryProfile;

/**
 * Wrapper class for per-node information in the query profile
 */
public class NodeWrapper {
  private final NodeQueryProfile nodeQueryProfile;

  public NodeWrapper(final NodeQueryProfile nodeQueryProfile, boolean includeDebugColumns) {
    this.nodeQueryProfile = nodeQueryProfile;
  }

  public static final String[] NODE_OVERVIEW_COLUMNS = {"Host Name", "Resource Waiting Time", "Peak Memory"};

  public void addSummary(TableBuilder tb) {
    tb.appendCell(nodeQueryProfile.getEndpoint().getAddress(), null);
    tb.appendMillis(nodeQueryProfile.getTimeEnqueuedBeforeSubmitMs());
    tb.appendBytes(nodeQueryProfile.getMaxMemoryUsed(), null);
  }

}
