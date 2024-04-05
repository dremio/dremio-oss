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
package org.apache.zookeeper.server.admin;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No op {@link AdminServer} provided to disable ZooKeeper HTTP functionality while not resorting to
 * the setting of System properties.
 */
public class JettyAdminServer implements AdminServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(JettyAdminServer.class);

  @Override
  public void start() throws AdminServerException {
    // Intentionally empty - see DX-53228
    LOGGER.debug(
        "zookeeper.admin.enableServer property ignored - ZooKeeper Admin server is disabled.");
  }

  @Override
  public void shutdown() throws AdminServerException {
    // Intentionally empty - see DX-53228
  }

  @Override
  public void setZooKeeperServer(ZooKeeperServer zooKeeperServer) {
    // Intentionally empty - see DX-53228
  }
}
