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
package com.dremio.dac.resource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;

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
      final NASConf nas = new NASConf();
      nas.path = folder.getRoot().getPath();
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

      newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), result.getTag());
    }
  }

  @Test
  public void testSourceHasDefaultTTL() throws Exception {
    final String sourceName = "src2";
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath();
    SourceUI source = new SourceUI();
    source.setName(sourceName);
    source.setCtime(System.currentTimeMillis());
    source.setConfig(nas);
    source.setAllowCrossSourceSelection(true);

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
    assertTrue(result.getAllowCrossSourceSelection());

    newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), result.getTag());
  }

  @Test
  public void testSourceHasDefaultRefreshPolicy() throws Exception {
    final String sourceName = "src3";
    final NASConf nas = new NASConf();
    nas.path = folder.getRoot().getPath() ;
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
    assertEquals(CatalogService.DEFAULT_AUTHTTLS_MILLIS, result.getMetadataPolicy().getAuthTTLMillis());
    assertEquals(CatalogService.DEFAULT_REFRESH_MILLIS, result.getMetadataPolicy().getNamesRefreshMillis());
    assertEquals(CatalogService.DEFAULT_REFRESH_MILLIS, result.getMetadataPolicy().getDatasetDefinitionRefreshAfterMillis());
    assertEquals(CatalogService.DEFAULT_EXPIRE_MILLIS, result.getMetadataPolicy().getDatasetDefinitionExpireAfterMillis());

    newNamespaceService().deleteSource(new SourcePath(sourceName).toNamespaceKey(), result.getTag());
  }

  @Test
  public void testHomeSourceCrossSelectOption() throws Exception {
    final SourceUI result = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__home"))).buildGet(),
      SourceUI.class
    );
    assertTrue(result.getAllowCrossSourceSelection());
    result.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__home")))
        .buildPut(Entity.entity(result, JSON)));
    final SourceUI updateResult = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__home"))).buildGet(),
      SourceUI.class
    );
    assertFalse(result.getAllowCrossSourceSelection());
    assertTrue(updateResult.getAllowCrossSourceSelection());

  }

  @Test
  public void testSystemSourceCrossSelectOption() throws Exception {
    final SourceUI resultAccel = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__accelerator"))).buildGet(),
      SourceUI.class
    );
    assertTrue(resultAccel.getAllowCrossSourceSelection());
    resultAccel.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__accelerator")))
        .buildPut(Entity.entity(resultAccel, JSON)));
    final SourceUI updateResultAccel = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__accelerator"))).buildGet(),
      SourceUI.class
    );
    assertFalse(resultAccel.getAllowCrossSourceSelection());
    assertTrue(updateResultAccel.getAllowCrossSourceSelection());

    final SourceUI resultJob = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__jobResultsStore"))).buildGet(),
      SourceUI.class
    );
    assertTrue(resultJob.getAllowCrossSourceSelection());
    resultJob.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__jobResultsStore")))
        .buildPut(Entity.entity(resultJob, JSON)));
    final SourceUI updateResultJob = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__jobResultsStore"))).buildGet(),
      SourceUI.class
    );
    assertFalse(resultJob.getAllowCrossSourceSelection());
    assertTrue(updateResultJob.getAllowCrossSourceSelection());

    final SourceUI resultScratch = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "$scratch"))).buildGet(),
      SourceUI.class
    );
    assertTrue(resultScratch.getAllowCrossSourceSelection());
    resultScratch.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "$scratch")))
        .buildPut(Entity.entity(resultScratch, JSON)));
    final SourceUI updateResultScratch = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "$scratch"))).buildGet(),
      SourceUI.class
    );
    assertFalse(resultScratch.getAllowCrossSourceSelection());
    assertTrue(updateResultScratch.getAllowCrossSourceSelection());

    final SourceUI resultDownload = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__datasetDownload"))).buildGet(),
      SourceUI.class
    );
    assertTrue(resultDownload.getAllowCrossSourceSelection());
    resultDownload.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__datasetDownload")))
        .buildPut(Entity.entity(resultDownload, JSON)));
    final SourceUI updateResultDownload = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__datasetDownload"))).buildGet(),
      SourceUI.class
    );
    assertFalse(resultDownload.getAllowCrossSourceSelection());
    assertTrue(updateResultDownload.getAllowCrossSourceSelection());

    final SourceUI resultSupport = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__support"))).buildGet(),
      SourceUI.class
    );
    assertTrue(resultSupport.getAllowCrossSourceSelection());
    resultSupport.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__support")))
        .buildPut(Entity.entity(resultSupport, JSON)));
    final SourceUI updateResultSupport = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__support"))).buildGet(),
      SourceUI.class
    );
    assertFalse(resultSupport.getAllowCrossSourceSelection());
    assertTrue(updateResultSupport.getAllowCrossSourceSelection());

    final SourceUI resultLogs = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__logs")).queryParam("includeContents", false)).buildGet(),
      SourceUI.class
    );
    assertTrue(resultLogs.getAllowCrossSourceSelection());
    resultLogs.setAllowCrossSourceSelection(false);
    expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__logs")))
        .buildPut(Entity.entity(resultLogs, JSON)));
    final SourceUI updateResultLogs = expectSuccess(
      getBuilder(getAPIv2().path(String.format("/source/%s", "__logs")).queryParam("includeContents", false)).buildGet(),
      SourceUI.class
    );
    assertFalse(resultLogs.getAllowCrossSourceSelection());
    assertTrue(updateResultLogs.getAllowCrossSourceSelection());

  }

}
