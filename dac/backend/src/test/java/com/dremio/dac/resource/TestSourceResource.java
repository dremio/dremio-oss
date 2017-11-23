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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.store.CatalogService;

/**
 * Tests {@link SourceResource} API
 */
public class TestSourceResource extends BaseTestServer {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testAddSourceWithAccelerationTTL() throws Exception {
    final String sourceName = "src";
    final long refreshPeriod = TimeUnit.HOURS.toMillis(4);
    final long gracePeriod = TimeUnit.HOURS.toMillis(12);
    {
      final NASConfig nas = new NASConfig();
      nas.setPath(folder.getRoot().getPath());
      SourceUI source = new SourceUI();
      source.setName(sourceName);
      source.setCtime(System.currentTimeMillis());
      source.setAccelerationRefreshPeriod(refreshPeriod);
      source.setAccelerationGracePeriod(gracePeriod);
      source.setConfig(nas);

      expectSuccess(
          getBuilder(getAPIv2().path(String.format("/source/%s", sourceName)))
              .buildPut(Entity.entity(source, JSON)));

      final SourceUI result = expectSuccess(
          getBuilder(getAPIv2().path(String.format("/source/%s", sourceName))).buildGet(),
          SourceUI.class
      );

      assertEquals(source.getFullPathList(), result.getFullPathList());
      assertEquals(source.getAccelerationRefreshPeriod(), result.getAccelerationRefreshPeriod());
      assertEquals(source.getAccelerationGracePeriod(), result.getAccelerationGracePeriod());

      newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), 0);
    }
  }

  @Test
  public void testSourceHasDefaultTTL() throws Exception {
    final String sourceName = "src2";
    final NASConfig nas = new NASConfig();
    nas.setPath(folder.getRoot().getPath());
    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(System.currentTimeMillis());
    source.setConfig(nas);

    expectSuccess(
        getBuilder(getAPIv2().path(String.format("/source/%s", sourceName)))
            .buildPut(Entity.entity(source, JSON)));

    final SourceUI result = expectSuccess(
        getBuilder(getAPIv2().path(String.format("/source/%s", sourceName))).buildGet(),
        SourceUI.class
    );

    assertEquals(source.getFullPathList(), result.getFullPathList());
    assertNotNull(result.getAccelerationRefreshPeriod());
    assertNotNull(result.getAccelerationGracePeriod());

    newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), 0);
  }



  @Test
  public void testSourceHasDefaultRefreshPolicy() throws Exception {
    final String sourceName = "src3";
    final NASConfig nas = new NASConfig();
    nas.setPath(folder.getRoot().getPath());
    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(System.currentTimeMillis());
    source.setConfig(nas);

    expectSuccess(
        getBuilder(getAPIv2().path(String.format("/source/%s", sourceName)))
            .buildPut(Entity.entity(source, JSON)));

    final SourceUI result = expectSuccess(
        getBuilder(getAPIv2().path(String.format("/source/%s", sourceName))).buildGet(),
        SourceUI.class
    );

    assertEquals(source.getFullPathList(), result.getFullPathList());

    assertNotNull(source.getMetadataPolicy());
    assertEquals(CatalogService.DEFAULT_EXPIRE_MILLIS, result.getMetadataPolicy().getAuthTTLMillis());
    assertEquals(CatalogService.DEFAULT_REFRESH_MILLIS, result.getMetadataPolicy().getNamesRefreshMillis());
    assertEquals(CatalogService.DEFAULT_REFRESH_MILLIS, result.getMetadataPolicy().getDatasetDefinitionRefreshAfterMillis());
    assertEquals(CatalogService.DEFAULT_EXPIRE_MILLIS, result.getMetadataPolicy().getDatasetDefinitionExpireAfterMillis());

    newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), 0);
  }
}
