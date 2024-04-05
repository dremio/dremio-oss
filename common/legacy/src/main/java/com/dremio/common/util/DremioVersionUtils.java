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

package com.dremio.common.util;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DremioVersionUtils {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DremioVersionUtils.class);

  /**
   * If given endpoint has dremio version compatible with coordinator version returns true.
   * Otherwise return false
   *
   * @param endpoint
   * @return
   */
  public static boolean isCompatibleVersion(NodeEndpoint endpoint) {
    boolean isCompatible = isCompatibleVersion(endpoint.getDremioVersion());
    logger.debug(
        "NodeEndpoint address:"
            + endpoint.getAddress()
            + ", NodeEndpoint version:"
            + endpoint.getDremioVersion()
            + ", Coordinator version:"
            + DremioVersionInfo.getVersion()
            + ", is NodeEndpoint compatible ?: "
            + isCompatible);
    return isCompatible;
  }

  public static boolean isCompatibleVersion(String version) {
    return DremioVersionInfo.getVersion().equals(version);
  }

  /**
   * Return collection of nodeEndpoints which are compatible with coordinator.
   *
   * @param nodeEndpoints
   * @return
   */
  public static Collection<NodeEndpoint> getCompatibleNodeEndpoints(
      Collection<NodeEndpoint> nodeEndpoints) {
    List<NodeEndpoint> compatibleNodeEndpoints = new ArrayList<>();
    if (nodeEndpoints != null && !nodeEndpoints.isEmpty()) {
      compatibleNodeEndpoints =
          nodeEndpoints.stream()
              .filter(nep -> isCompatibleVersion(nep))
              .collect(Collectors.toList());
    }
    return Collections.unmodifiableCollection(compatibleNodeEndpoints);
  }
}
