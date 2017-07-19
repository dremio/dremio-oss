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
package com.dremio.dac.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.proto.model.source.MapRFSConfig;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * To test private fields not being visible/transferrable
 */
public class TestSourcePrivateFields extends BaseTestServer {

  @Test
  public void testPrivateFieldsSource() throws Exception {
    final SourceUI source = new SourceUI();
    final S3Config s3Config = new S3Config();
    s3Config.setAccessKey("ABCDEFG");
    s3Config.setAccessSecret("HIKLMNOP");
    s3Config.setSecure(true);
    s3Config.setExternalBucketList(Arrays.asList(new String[] {"abc", "bcd", "efg"}));
    source.setConfig(s3Config);

    final NamespaceService namespaceService = newNamespaceService();
    final NamespaceKey nameSpaceKey = new SourcePath("src").toNamespaceKey();

    namespaceService.addOrUpdateSource(nameSpaceKey, source.asSourceConfig());

    final SourceConfig config = namespaceService.getSource(nameSpaceKey);

    SourceUI mySource = SourceUI.get(config);
    S3Config myS3config = (S3Config) mySource.getConfig();

    assertNull(myS3config.getAccessKey());
    assertNull(myS3config.getAccessSecret());

    assertNotNull(myS3config.getSecure());
    assertNotNull(myS3config.getExternalBucketList());

    assertEquals(true, myS3config.getSecure());
    assertEquals(Arrays.asList(new String[] {"abc", "bcd", "efg"}), myS3config.getExternalBucketList());

    SourceUI withPrivatesSource = SourceUI.getWithPrivates(config);
    S3Config withPrivateS3Config = (S3Config) withPrivatesSource.getConfig();

    assertNotNull(withPrivateS3Config.getAccessSecret());
    assertNotNull(withPrivateS3Config.getAccessKey());

    assertEquals("ABCDEFG", withPrivateS3Config.getAccessKey());
    assertEquals("HIKLMNOP", withPrivateS3Config.getAccessSecret());

    assertNotNull(withPrivateS3Config.getSecure());
    assertNotNull(withPrivateS3Config.getExternalBucketList());

    assertEquals(true, withPrivateS3Config.getSecure());
    assertEquals(Arrays.asList(new String[] {"abc", "bcd", "efg"}), withPrivateS3Config.getExternalBucketList());
  }

  @Test
  public void testPrivatesNoPrivateFields() throws Exception {
    final MapRFSConfig maprfsConfig = new MapRFSConfig();
    maprfsConfig.setEnableImpersonation(true);
    maprfsConfig.setClusterName("my.cluster.com");
    maprfsConfig.setSecure(true);

    SourceUI source = new SourceUI();
    source.setConfig(maprfsConfig);

    final NamespaceService namespaceService = newNamespaceService();
    final NamespaceKey nameSpaceKey = new SourcePath("srcA").toNamespaceKey();

    namespaceService.addOrUpdateSource(nameSpaceKey, source.asSourceConfig());

    final SourceConfig config = namespaceService.getSource(nameSpaceKey);

    MapRFSConfig mySource = (MapRFSConfig) SourceUI.get(config).getConfig();
    MapRFSConfig withPrivates = (MapRFSConfig) SourceUI.getWithPrivates(config).getConfig();

    assertEquals(mySource, withPrivates);
  }
}
