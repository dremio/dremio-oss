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
package com.dremio.dac.api;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.dremio.dac.model.sources.SourcePath;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;

/**
 * Tests {@link SourceResource} API
 */
public class TestSourceResource extends BaseTestServer {
  private static final String SOURCES_PATH = "/source/";

  @Test
  public void testListSources() throws Exception {
    ResponseList<Source> sources = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildGet(), new GenericType<ResponseList<Source>>() {});
    assertEquals(sources.getData().size(), newSourceService().getSources().size());
  }

  @Test
  public void testAddSource() throws Exception {
    Source newSource = new Source();
    newSource.setName("Foopy");
    newSource.setType("NAS");
    NASConfig config = new NASConfig();
    config.setPath("/");
    newSource.setConfig(config);

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildPost(Entity.entity(newSource, JSON)), Source.class);
    assertEquals(source.getName(), newSource.getName());

    newNamespaceService().deleteSource(new SourcePath(source.getName()).toNamespaceKey(), 0);
  }

  @Test
  public void testAddSourceErrors() throws Exception {
    // test invalid sources
    Source newSource = new Source();

    // no config
    newSource.setName("Foobar");
    newSource.setType("NAS");
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildPost(Entity.entity(newSource, JSON)));

    // invalid name
    newSource.setName("f");
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH)).buildPost(Entity.entity(newSource, JSON)));
  }

  @Test
  public void testUpdateSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy2");
    sourceConfig.setType(SourceType.NAS);

    NASConfig nasConfig = new NASConfig();
    nasConfig.setPath("/");

    sourceConfig.setConfig(nasConfig.toByteString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig, nasConfig);

    Source updatedSource = new Source(createdSourceConfig);
    updatedSource.setDescription("Desc");

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)), Source.class);

    assertEquals(source.getDescription(), "Desc");
    assertEquals(source.getTag(), "1");

    newNamespaceService().deleteSource(new SourcePath(source.getName()).toNamespaceKey(), 1);
  }

  @Test
  public void testUpdateSourceErrors() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy5");
    sourceConfig.setType(SourceType.NAS);

    NASConfig nasConfig = new NASConfig();
    nasConfig.setPath("/");

    sourceConfig.setConfig(nasConfig.toByteString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig, nasConfig);

    Source updatedSource = new Source(createdSourceConfig);

    // test updating non-existent source
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildPut(Entity.entity(updatedSource, JSON)));

    // test wrong tag
    updatedSource.setTag("badtag");
    expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildPut(Entity.entity(updatedSource, JSON)));

    newNamespaceService().deleteSource(new SourcePath(updatedSource.getName()).toNamespaceKey(), 0);
  }

  @Test
  public void testGetSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy4");
    sourceConfig.setType(SourceType.NAS);

    NASConfig nasConfig = new NASConfig();
    nasConfig.setPath("/");

    sourceConfig.setConfig(nasConfig.toByteString());
    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig, nasConfig);

    Source source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildGet(), Source.class);

    assertEquals(source.getName(), sourceConfig.getName());
  }

  @Test
  public void testGetNonExistingSource() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildGet());
  }

  @Test
  public void testDeleteSource() throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy3");
    sourceConfig.setType(SourceType.NAS);

    NASConfig nasConfig = new NASConfig();
    nasConfig.setPath("/");

    sourceConfig.setConfig(nasConfig.toByteString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig, nasConfig);

    expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildDelete());
  }

  @Test
  public void testDeleteNoneExistingSource() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("nonexistandid")).buildDelete());
  }
}
