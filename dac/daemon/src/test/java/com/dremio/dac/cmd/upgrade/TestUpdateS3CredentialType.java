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
package com.dremio.dac.cmd.upgrade;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ConnectionReaderImpl;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.plugins.s3.store.S3PluginConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceStore;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

/** Test class for {@code UpdateS3CredentialType} */
public class TestUpdateS3CredentialType extends DremioTest {
  @Test
  public void checkUpdateNullToNone() throws Exception {
    S3PluginConfig s3PluginConfig = new S3PluginConfig();
    s3PluginConfig.accessKey = null;
    checkUpdateHelper(s3PluginConfig, AWSAuthenticationType.NONE);
  }

  @Test
  public void checkUpdateEmptyToNone() throws Exception {
    S3PluginConfig s3PluginConfig = new S3PluginConfig();
    s3PluginConfig.accessKey = "";
    checkUpdateHelper(s3PluginConfig, AWSAuthenticationType.NONE);
  }

  @Test
  public void checkUpdateKeepAccessKey() throws Exception {
    S3PluginConfig s3PluginConfig = new S3PluginConfig();
    s3PluginConfig.accessKey = "ACCESS_KEY";
    s3PluginConfig.accessSecret = SecretRef.of("ACCESS_SECRET");
    checkUpdateHelper(s3PluginConfig, AWSAuthenticationType.ACCESS_KEY);
  }

  private void checkUpdateHelper(
      S3PluginConfig s3OldPluginConfig, AWSAuthenticationType authenticationType) throws Exception {
    try (final LocalKVStoreProvider kvStoreProvider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false)) {
      kvStoreProvider.start();
      final LegacyKVStoreProvider legacyKVStoreProvider = kvStoreProvider.asLegacy();
      NamespaceStore namespace = new NamespaceStore(() -> kvStoreProvider);
      newS3Source(namespace, "s3 plugin config", s3OldPluginConfig);
      // Performing upgrade
      UpdateS3CredentialType task = new UpdateS3CredentialType();
      final LogicalPlanPersistence lpPersistence =
          new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
      final ConnectionReader connectionReader =
          ConnectionReader.of(CLASSPATH_SCAN_RESULT, ConnectionReaderImpl.class);
      UpgradeContext context =
          new UpgradeContext(
              kvStoreProvider, legacyKVStoreProvider, lpPersistence, connectionReader, null);
      task.upgrade(context);

      final NamespaceService namespaceService =
          new NamespaceServiceImpl(
              context.getKvStoreProvider(),
              new CatalogStatusEventsImpl(),
              CatalogEventMessagePublisherProvider.NO_OP);
      List<SourceConfig> sources = namespaceService.getSources();
      assertEquals(1, sources.size());

      ConnectionConf<?, ?> connectionConf =
          context.getConnectionReader().getConnectionConf(sources.get(0));
      assertTrue(connectionConf instanceof S3PluginConfig);

      S3PluginConfig s3PluginConfig = (S3PluginConfig) connectionConf;
      assertEquals(authenticationType, s3PluginConfig.credentialType);
    }
  }

  private void newS3Source(NamespaceStore namespace, String path, S3PluginConfig s3PluginConfig) {
    final List<String> fullPathList = Arrays.asList(path);

    final SourceConfig config =
        new SourceConfig()
            .setId(new EntityId(UUID.randomUUID().toString()))
            .setName(path)
            .setConnectionConf(s3PluginConfig);

    namespace.put(
        NamespaceServiceImpl.getKey(new NamespaceKey(fullPathList)),
        new NameSpaceContainer()
            .setFullPathList(fullPathList)
            .setType(NameSpaceContainer.Type.SOURCE)
            .setSource(config));
  }
}
