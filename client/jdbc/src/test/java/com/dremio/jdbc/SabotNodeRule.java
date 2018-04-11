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
package com.dremio.jdbc;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotNode;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.util.TestUtilities;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.typesafe.config.ConfigValueFactory;

/**
 * A rule to manage a small Sabot node server to be used for jdbc tests
 */
public class SabotNodeRule extends ExternalResource {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotNodeRule.class);

  private static final SabotConfig dConfig = SabotConfig
      .create()
      .withValue(ExecConstants.HTTP_ENABLE, ConfigValueFactory.fromAnyRef(false));

  // rule own private temporary folder for execution
  private final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ClusterCoordinator coordinator;
  private SabotNode node;

  public SabotNodeRule() {

  }

  public String getJDBCConnectionString() {
    NodeEndpoint endpoint = node.getContext().getEndpoint();
    return String.format("jdbc:dremio:direct=localhost:%d", endpoint.getUserPort());
  }

  @Override
  public Statement apply(Statement base, Description description) {
    // Wrap statement with temporary folder one
    // temporary folder will be executed first later, and then this class, and then base
    return temporaryFolder.apply(super.apply(base, description), description);
  }

  @Override
  protected void before() throws Throwable {
    this.coordinator = new LocalClusterCoordinator();
    coordinator.start();
    node = new SabotNode(dConfig, coordinator);
    node.run();

    final String tmpDirPath = temporaryFolder.newFolder().getPath();
    TestUtilities.addDefaultTestPlugins(node.getBindingProvider().lookup(CatalogService.class), tmpDirPath);

    super.before();
  }


  @Override
  protected void after() {
    try {
      super.after();
    } finally {
      if (node != null) {
        node.close();
        node = null;
      }
      if (coordinator != null) {
        try {
          coordinator.close();
        } catch (Exception e) {
          logger.error("Could not stop sabot node server", e);
        }
        coordinator = null;
      }
    }
  }
}
