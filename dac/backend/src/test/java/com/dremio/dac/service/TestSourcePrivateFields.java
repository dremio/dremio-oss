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
package com.dremio.dac.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ConnectionReaderImpl;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import org.junit.Test;

/** To test private fields not being visible/transferrable */
public class TestSourcePrivateFields extends BaseTestServer {
  private final ConnectionReader connectionReader =
      ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, ConnectionReaderImpl.class);

  @Test
  public void testPrivateFieldsSource() throws Exception {
    final SourceUI source = new SourceUI();
    final APrivateSource priv = new APrivateSource();
    priv.password = "ABC";
    priv.username = "hello";
    source.setConfig(priv);

    final NamespaceService namespaceService = getNamespaceService();
    final NamespaceKey nameSpaceKey = new SourcePath("src").toNamespaceKey();

    namespaceService.addOrUpdateSource(nameSpaceKey, source.asSourceConfig());

    final SourceConfig config = namespaceService.getSource(nameSpaceKey);

    SourceUI mySource = SourceUI.get(config, connectionReader);
    APrivateSource myS3config = (APrivateSource) mySource.getConfig();

    assertEquals(myS3config.password, ConnectionConf.USE_EXISTING_SECRET_VALUE);
    assertNotNull(myS3config.username);

    SourceUI withPrivatesSource = SourceUI.getWithPrivates(config, connectionReader);
    APrivateSource withPrivateS3Config = (APrivateSource) withPrivatesSource.getConfig();

    assertNotNull(withPrivateS3Config.password);
    assertNotNull(withPrivateS3Config.username);

    assertEquals("ABC", withPrivateS3Config.password);
    assertEquals("hello", withPrivateS3Config.username);

    namespaceService.deleteSource(nameSpaceKey, config.getTag());
  }

  @Test
  public void testEmptySecretsShouldNotBeReplaced() throws NamespaceException {
    final SourceUI source = new SourceUI();
    final APrivateSource priv = new APrivateSource();
    priv.username = "hello";
    source.setConfig(priv);

    final NamespaceService namespaceService = getNamespaceService();
    final NamespaceKey nameSpaceKey = new SourcePath("src").toNamespaceKey();

    namespaceService.addOrUpdateSource(nameSpaceKey, source.asSourceConfig());

    final SourceConfig config = namespaceService.getSource(nameSpaceKey);

    SourceUI mySource = SourceUI.get(config, connectionReader);
    APrivateSource myS3config = (APrivateSource) mySource.getConfig();

    assertNull(myS3config.password);

    namespaceService.deleteSource(nameSpaceKey, config.getTag());
  }

  @Test
  public void testPrivatesNoPrivateFields() throws Exception {
    final NASConf nasConf = new NASConf();
    nasConf.path = "/mypath/";
    SourceUI source = new SourceUI();
    source.setConfig(nasConf);

    final NamespaceService namespaceService = getNamespaceService();
    final NamespaceKey nameSpaceKey = new SourcePath("srcA").toNamespaceKey();

    namespaceService.addOrUpdateSource(nameSpaceKey, source.asSourceConfig());

    final SourceConfig config = namespaceService.getSource(nameSpaceKey);

    NASConf mySource = (NASConf) SourceUI.get(config, connectionReader).getConfig();
    NASConf withPrivates = (NASConf) SourceUI.getWithPrivates(config, connectionReader).getConfig();

    assertEquals(mySource, withPrivates);
  }
}
