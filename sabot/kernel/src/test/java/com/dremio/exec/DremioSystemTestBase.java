/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec;

import static com.google.common.base.Throwables.propagate;

import java.util.List;

import org.slf4j.Logger;

import com.dremio.exec.exception.NodeStartupException;
import com.dremio.exec.server.SabotNode;
import com.google.common.collect.ImmutableList;

/**
 * Base class for Dremio system tests.
 * Starts one or more SabotNodes, an embedded ZooKeeper cluster and provides a configured client for testing.
 */
public class DremioSystemTestBase extends TestWithZookeeper {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(DremioSystemTestBase.class);

  private List<SabotNode> servers;

  public void startCluster(int numServers) {
    try {
      ImmutableList.Builder<SabotNode> servers = ImmutableList.builder();
      for (int i = 0; i < numServers; i++) {
        servers.add(SabotNode.start(getConfig()));
      }
      this.servers = servers.build();
    } catch (NodeStartupException e) {
      propagate(e);
    }
  }

  public void stopCluster() {
    if (servers != null) {
      for (SabotNode server : servers) {
        try {
          server.close();
        } catch (Exception e) {
          logger.warn("Error shutting down SabotNode", e);
        }
      }
    }
  }

  public SabotNode getABit(){
    return this.servers.iterator().next();
  }
}
