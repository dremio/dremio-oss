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
package com.dremio.dac.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.APrivateSource;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.dataset.proto.AccelerationSettings;
import com.dremio.service.namespace.dataset.proto.RefreshMethod;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;

/**
 * Tests {@link SourceResource} API
 */
public class TestSourceResource extends BaseTestServer {
  private static final String SOURCES_PATH = "/source/";

  private final ConnectionReader reader = new ConnectionReader(DremioTest.CLASSPATH_SCAN_RESULT);

  @Test
  public void testListSources() throws Exception {
    ResponseList<SourceResource.SourceDeprecated> sources = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildGet(), new GenericType<ResponseList<SourceResource.SourceDeprecated>>() {});
    assertEquals(sources.getData().size(), newSourceService().getSources().size());
  }

  @Test
  public void testAddSource() throws Exception {
    SourceResource.SourceDeprecated newSource = new SourceResource.SourceDeprecated();
    newSource.setName("Foopy");
    newSource.setType("NAS");
    NASConf config = new NASConf();
    config.path = "/";
    newSource.setConfig(config);

    SourceResource.SourceDeprecated source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildPost(Entity.entity(newSource, JSON)), SourceResource.SourceDeprecated.class);
    assertEquals(source.getName(), newSource.getName());
    assertNotNull(source.getState());

    newNamespaceService().deleteSource(new SourcePath(source.getName()).toNamespaceKey(), 0);
  }

  @Test
  public void testAddSourceErrors() throws Exception {
    // test invalid sources
    SourceResource.SourceDeprecated newSource = new SourceResource.SourceDeprecated();

    // no config
    newSource.setName("Foobar");
    newSource.setType("NAS");
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildPost(Entity.entity(newSource, JSON)));
  }

  @Test
  public void testUpdateSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy2");

    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    final AccelerationSettings settings = new AccelerationSettings()
      .setMethod(RefreshMethod.FULL)
      .setRefreshPeriod(TimeUnit.HOURS.toMillis(2))
      .setGracePeriod(TimeUnit.HOURS.toMillis(6));
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader);
    updatedSource.setDescription("Desc");

    SourceResource.SourceDeprecated source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)), SourceResource.SourceDeprecated.class);

    assertEquals(source.getDescription(), "Desc");
    assertEquals(source.getTag(), "1");
    assertNotNull(source.getState());

    newNamespaceService().deleteSource(new SourcePath(source.getName()).toNamespaceKey(), 1);
  }

  @Test
  public void testUpdateSourceErrors() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy5");
    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    final AccelerationSettings settings = new AccelerationSettings()
      .setMethod(RefreshMethod.FULL)
      .setRefreshPeriod(TimeUnit.HOURS.toMillis(2))
      .setGracePeriod(TimeUnit.HOURS.toMillis(6));
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader);

    // test updating non-existent source
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildPut(Entity.entity(updatedSource, JSON)));

    // test wrong tag
    updatedSource.setTag("badtag");
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildPut(Entity.entity(updatedSource, JSON)));

    newNamespaceService().deleteSource(new SourcePath(updatedSource.getName()).toNamespaceKey(), 0);
  }

  @Test
  public void testUpdateSourceNegativeDatasetRefresh() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy2");

    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    final AccelerationSettings settings = new AccelerationSettings()
      .setMethod(RefreshMethod.FULL)
      .setRefreshPeriod(TimeUnit.HOURS.toMillis(2))
      .setGracePeriod(TimeUnit.HOURS.toMillis(6));

    // Negative data refresh interval -- should trigger a bad request.
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader);
    updatedSource.getMetadataPolicy().setDatasetRefreshAfterMs(-1L);

    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)));
    newNamespaceService().deleteSource(new SourcePath(createdSourceConfig.getName()).toNamespaceKey(), 1);
  }

  @Test
  public void testUpdateSourceNegativeNameRefresh() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy2");

    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    final AccelerationSettings settings = new AccelerationSettings()
      .setMethod(RefreshMethod.FULL)
      .setRefreshPeriod(TimeUnit.HOURS.toMillis(2))
      .setGracePeriod(TimeUnit.HOURS.toMillis(6));

    // Negative name refresh interval -- should trigger a bad request.
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader);
    updatedSource.getMetadataPolicy().setNamesRefreshMs(-1L);

    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)));
    newNamespaceService().deleteSource(new SourcePath(createdSourceConfig.getName()).toNamespaceKey(), 1);
  }

  @Test
  public void testGetSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy4");
    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());
    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    SourceResource.SourceDeprecated source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildGet(), SourceResource.SourceDeprecated.class);

    assertEquals(source.getName(), sourceConfig.getName());
    assertNotNull(source.getState());
  }

  @Test
  public void testGetNonExistingSource() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildGet());
  }

  @Test
  public void testDeleteSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy3");

    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildDelete());
  }

  @Test
  public void testDeleteNoneExistingSource() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("nonexistandid")).buildDelete());
  }

  @Test
  public void testRemovingSensitiveFields() throws Exception {
    SourceConfig config = new SourceConfig();
    config.setName("Foopy");
    config.setId(new EntityId("id"));
    config.setVersion(0L);
    config.setAccelerationGracePeriod(0L);
    config.setAccelerationRefreshPeriod(0L);

    APrivateSource priv = new APrivateSource();
    priv.password = "hello";
    config.setConnectionConf(priv);

    SourceResource sourceResource = new SourceResource(newSourceService());
    SourceResource.SourceDeprecated source = sourceResource.fromSourceConfig(config);
    APrivateSource newConfig = (APrivateSource) source.getConfig();

    // make sure the sensitive fields have been removed
    assertNull(newConfig.password);
  }
}
