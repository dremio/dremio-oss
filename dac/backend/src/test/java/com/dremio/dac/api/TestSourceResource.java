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
package com.dremio.dac.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.service.APrivateSource;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.CatalogService;
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

  private final ConnectionReader reader = ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG);

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

    deleteSource(source.getName());
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
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader, null);
    updatedSource.setDescription("Desc");

    SourceResource.SourceDeprecated source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)), SourceResource.SourceDeprecated.class);

    assertEquals("Desc", source.getDescription());
    assertNotNull(source.getState());
    assertNotNull(source.getTag());
    deleteSource(source.getName());
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
    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader, null);

    // test updating non-existent source
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("badid")).buildPut(Entity.entity(updatedSource, JSON)));

    // test wrong tag
    updatedSource.setTag("badtag");
    expectStatus(Response.Status.CONFLICT, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)));

    deleteSource(updatedSource.getName());
  }

  @Test
  public void testUpdateSourceNegativeDatasetRefresh() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setDatasetRefreshAfterMs(-1L);
    testMetadataPolicyWithInvalidValues(testPolicy);
  }

  @Test
  public void testUpdateSourceBoundaryValues() throws Exception {
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

    SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader, null);
    updatedSource.getMetadataPolicy().setDatasetRefreshAfterMs(MetadataPolicy.ONE_MINUTE_IN_MS);
    updatedSource.getMetadataPolicy().setAuthTTLMs(MetadataPolicy.ONE_MINUTE_IN_MS);
    updatedSource.getMetadataPolicy().setNamesRefreshMs(MetadataPolicy.ONE_MINUTE_IN_MS);

    SourceResource.SourceDeprecated source = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)), SourceResource.SourceDeprecated.class);
    assertEquals(source.getMetadataPolicy().getAuthTTLMs(), updatedSource.getMetadataPolicy().getAuthTTLMs());
    assertEquals(source.getMetadataPolicy().getDatasetRefreshAfterMs(), updatedSource.getMetadataPolicy().getDatasetRefreshAfterMs());
    assertEquals(source.getMetadataPolicy().getNamesRefreshMs(), updatedSource.getMetadataPolicy().getNamesRefreshMs());

    deleteSource(createdSourceConfig.getName());
  }

  @Test
  public void testUpdateSourceLowDatasetRefresh() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setDatasetRefreshAfterMs(500);
    testMetadataPolicyWithInvalidValues(testPolicy);
  }

  @Test
  public void testUpdateSourceLowDatasetExpire() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setDatasetExpireAfterMs(500);
    testMetadataPolicyWithInvalidValues(testPolicy);
  }

  @Test
  public void testUpdateLowAuthTTL() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setAuthTTLMs(500L);
    testMetadataPolicyWithInvalidValues(testPolicy);
  }

  @Test
  public void testUpdateLowNamesRefresh() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setNamesRefreshMs(500L);
    testMetadataPolicyWithInvalidValues(testPolicy);
  }

  @Test
  public void testUpdateSourceNegativeNameRefresh() throws Exception {
    final MetadataPolicy testPolicy = new MetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    testPolicy.setNamesRefreshMs(-1L);
    testMetadataPolicyWithInvalidValues(testPolicy);
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
    config.setTag("0");
    config.setAccelerationGracePeriod(0L);
    config.setAccelerationRefreshPeriod(0L);

    APrivateSource priv = new APrivateSource();
    priv.password = "hello";
    config.setConnectionConf(priv);

    SourceResource sourceResource = new SourceResource(newSourceService(), null);
    SourceResource.SourceDeprecated source = sourceResource.fromSourceConfig(config);
    APrivateSource newConfig = (APrivateSource) source.getConfig();

    // make sure the sensitive fields have been removed
    assertEquals(newConfig.password, ConnectionConf.USE_EXISTING_SECRET_VALUE);
  }

  @Test
  public void testSourcesByType() throws Exception {
    ResponseList<SourceTypeTemplate> types = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("type")).buildGet(), new GenericType<ResponseList<SourceTypeTemplate>>() {});

    Boolean found = false;

    for (SourceTypeTemplate type : types.getData()) {
      if (type.getSourceType().equals("FAKESOURCE")) {
        found = true;

        // check that we load the svg image for the source
        assertNotNull(type.getIcon());
        assertTrue(type.getIcon().contains("FakeSVG"));
        assertEquals(type.getLabel(), "FakeSource");
      } else {
        // test that we can load each source type that is discoverable
        expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("type").path(type.getSourceType())).buildGet(), SourceTypeTemplate.class);
      }
    }

    assertTrue(found);
  }

  @Test
  public void testSourceByType() throws Exception {
    SourceTypeTemplate type = expectSuccess(getBuilder(getPublicAPI(3).path(SOURCES_PATH).path("type").path("FAKESOURCE")).buildGet(), SourceTypeTemplate.class);

    assertEquals(type.getSourceType(), "FAKESOURCE");
    assertEquals(type.getLabel(), "FakeSource");

    List<SourcePropertyTemplate> elements = type.getElements();

    assertEquals(elements.size(), 9);

    assertSourceProperty(elements.get(0), "username", "text", null, false, null);
    assertSourceProperty(elements.get(1), "password", "text", null, true, null);
    assertSourceProperty(elements.get(2), "numeric", "number", null, false, 4);
    assertSourceProperty(elements.get(3), "isAwesome", "boolean", "Awesome!", false, true);
    assertSourceProperty(elements.get(4), "valueList", "value_list", null, false, null);
    assertSourceProperty(elements.get(5), "hostList", "host_list", null, false, null);
    assertSourceProperty(elements.get(6), "propList", "property_list", null, false, null);
    assertSourceProperty(elements.get(7), "authenticationType", "credentials", null, false, null);
    assertSourceProperty(elements.get(8), "enumType", "enum", null, false, null);

    List<SourcePropertyTemplate.EnumValueTemplate> options = elements.get(8).getOptions();

    assertEquals(options.size(), 2);
    assertEquals(options.get(0).getValue(), "ENUM_1");
    assertEquals(options.get(0).getLabel(), "Enum #1");
    assertEquals(options.get(1).getValue(), "ENUM_2");
    assertEquals(options.get(1).getLabel(), "Enum #2");
  }

  private void assertSourceProperty(SourcePropertyTemplate property, String propertyName, String type, String label, boolean isSecret, Object value) {
    assertEquals(property.getPropertyName(), propertyName);
    assertEquals(property.getType(), type);
    assertEquals(property.getLabel(), label);
    assertEquals(property.getSecret(), isSecret);
    assertEquals(property.getDefaultValue(), value);
  }

  private void testMetadataPolicyWithInvalidValues(MetadataPolicy policy) throws Exception {
    SourceConfig sourceConfig = new SourceConfig();
    sourceConfig.setName("Foopy2");

    NASConf nasConfig = new NASConf();
    sourceConfig.setType(nasConfig.getType());
    nasConfig.path = "/";

    sourceConfig.setConfig(nasConfig.toBytesString());

    SourceConfig createdSourceConfig = newSourceService().registerSourceWithRuntime(sourceConfig);

    try {
      final AccelerationSettings settings = new AccelerationSettings()
        .setMethod(RefreshMethod.FULL)
        .setRefreshPeriod(TimeUnit.HOURS.toMillis(2))
        .setGracePeriod(TimeUnit.HOURS.toMillis(6));

      SourceResource.SourceDeprecated updatedSource = new SourceResource.SourceDeprecated(createdSourceConfig, settings, reader, null);

      updatedSource.getMetadataPolicy().setDatasetRefreshAfterMs(policy.getDatasetRefreshAfterMs());
      updatedSource.getMetadataPolicy().setDatasetExpireAfterMs(policy.getDatasetExpireAfterMs());
      updatedSource.getMetadataPolicy().setAuthTTLMs(policy.getAuthTTLMs());
      updatedSource.getMetadataPolicy().setNamesRefreshMs(policy.getNamesRefreshMs());

      expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(SOURCES_PATH).path(createdSourceConfig.getId().getId())).buildPut(Entity.entity(updatedSource, JSON)));
    } finally {
      deleteSource(createdSourceConfig.getName());
    }
  }
}
