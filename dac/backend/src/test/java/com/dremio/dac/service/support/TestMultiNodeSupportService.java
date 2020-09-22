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
package com.dremio.dac.service.support;

import java.io.File;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.proto.model.source.ClusterIdentity;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.support.SupportService;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.io.Files;
import com.google.common.io.Resources;


/**
 * Test the support service
 */
public class TestMultiNodeSupportService extends BaseTestServer {

  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  @ClassRule
  public static final TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass // same signature to shadow parent's #init
  public static void init() throws Exception {
    // set the log path so we can read logs and confirm that is working.
    final File jsonFolder = temp.newFolder("json");
    jsonFolder.mkdir();
    Files.copy(new File(Resources.getResource("support/server.json").getPath()), new File(jsonFolder, "server.json"));
    System.setProperty(SupportService.DREMIO_LOG_PATH_PROPERTY, temp.getRoot().toString());
    System.setProperty("dremio_multinode", "true");
    System.setProperty("dremio.service.jobs.over_socket", "true");

    // now start server.
    BaseTestServer.init();
  }

  @Test
  public void getClusterIdOnExecutor() throws Exception {
    SupportService masterSupport = getMasterDremioDaemon().getBindingProvider().lookup(SupportService.class);
    ClusterIdentity masterId = masterSupport.getClusterId();

    SupportService executorSupport = getExecutorDaemon().getBindingProvider().lookup(SupportService.class);
    ClusterIdentity executorId = executorSupport.getClusterId();

    Assert.assertTrue(masterId.equals(executorId));
  }
}
