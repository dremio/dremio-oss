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
package com.dremio.plugins.icebergcatalog.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.ConnectionReaderImpl;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that the generic Iceberg REST catalog source is registered as {@code RESTCATALOG} and
 * that the public REST API JSON shape (as documented for Dremio's RESTCATALOG source) can be
 * deserialized into {@link RestIcebergCatalogPluginConfig}.
 */
public class TestRestCatalogSourceRegistration extends DremioTest {

  private static final String RESTCATALOG = "RESTCATALOG";
  private static final String TEST_CLIENT_SECRET = "test-client-secret-value-0001";

  private static ConnectionReader connectionReader;

  /** Mirrors the shape of {@code com.dremio.dac.api.Source} (type is an external property). */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SourceJsonHolder {
    private String entityType;
    private String name;

    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
        property = "type")
    private ConnectionConf<?, ?> config;

    public String getEntityType() {
      return entityType;
    }

    public void setEntityType(String entityType) {
      this.entityType = entityType;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public ConnectionConf<?, ?> getConfig() {
      return config;
    }

    public void setConfig(ConnectionConf<?, ?> config) {
      this.config = config;
    }
  }

  @BeforeClass
  public static void setUpReader() {
    connectionReader =
        ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, ConnectionReaderImpl.class);
  }

  private static ObjectMapper newRegisteredMapper() {
    ObjectMapper mapper = new ObjectMapper();
    ConnectionConf.registerSubTypes(mapper, connectionReader);
    return mapper;
  }

  private static RestIcebergCatalogPluginConfig newPolarisLikeConfig() {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "test_catalog"),
                new Property("scope", "PRINCIPAL_ROLE:ALL")));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(new Property("credential", "test-client-id:" + TEST_CLIENT_SECRET)));
    return conf;
  }

  @Test
  public void testRestCatalogTypeIsRegistered() {
    assertSame(
        RestIcebergCatalogPluginConfig.class,
        connectionReader.getAllConnectionConfs().get(RESTCATALOG));
  }

  @Test
  public void testSourceTypeAnnotation() {
    SourceType sourceType = RestIcebergCatalogPluginConfig.class.getAnnotation(SourceType.class);
    assertNotNull("RestIcebergCatalogPluginConfig must be annotated with @SourceType", sourceType);
    assertEquals(RESTCATALOG, sourceType.value());
    assertEquals("Iceberg REST Catalog", sourceType.label());
    assertEquals("restcatalog-layout.json", sourceType.uiConfig());
    assertTrue(sourceType.configurable());
    assertFalse(sourceType.isVersioned());
  }

  @Test
  public void testGetTypeIsRestCatalog() {
    assertEquals(RESTCATALOG, new RestIcebergCatalogPluginConfig().getType());
  }

  @Test
  public void testExactlyOneClassClaimsRestCatalogType() {
    // A Polaris preset must stay UI-only: a second @SourceType("RESTCATALOG") breaks startup.
    List<String> claimants = new ArrayList<>();
    for (Class<?> clazz : DremioTest.CLASSPATH_SCAN_RESULT.getAnnotatedClasses(SourceType.class)) {
      SourceType sourceType = clazz.getAnnotation(SourceType.class);
      if (sourceType != null && RESTCATALOG.equals(sourceType.value())) {
        claimants.add(clazz.getName());
      }
    }
    assertEquals(Arrays.asList(RestIcebergCatalogPluginConfig.class.getName()), claimants);
  }

  @Test
  public void testNoPolarisSpecificSourceType() {
    for (String type : connectionReader.getAllConnectionConfs().keySet()) {
      assertFalse(
          "Polaris must not have its own source type: " + type,
          type.toUpperCase(Locale.ROOT).contains("POLARIS"));
    }
  }

  @Test
  public void testReaderDeserializesBytesByType() {
    RestIcebergCatalogPluginConfig conf = newPolarisLikeConfig();
    conf.isUsingVendedCredentials = true;

    ConnectionConf<?, ?> read =
        connectionReader.getConnectionConf(RESTCATALOG, conf.toBytesString());

    assertTrue(read instanceof RestIcebergCatalogPluginConfig);
    RestIcebergCatalogPluginConfig restConf = (RestIcebergCatalogPluginConfig) read;
    assertEquals(conf.restEndpointUri, restConf.restEndpointUri);
    assertEquals(conf.propertyList, restConf.propertyList);
    assertEquals(conf.secretPropertyList, restConf.secretPropertyList);
    assertTrue(restConf.isUsingVendedCredentials);
    assertEquals(conf, restConf);
  }

  @Test
  public void testReaderResolvesSourceConfigWithoutFallingBackToMissingPlugin() {
    SourceConfig sourceConfig =
        new SourceConfig()
            .setName("polaris")
            .setType(RESTCATALOG)
            .setConfig(newPolarisLikeConfig().toBytesString());

    ConnectionConf<?, ?> conf = connectionReader.getConnectionConf(sourceConfig);

    assertTrue(
        "Expected RestIcebergCatalogPluginConfig but got " + conf.getClass().getName(),
        conf instanceof RestIcebergCatalogPluginConfig);
    assertEquals(RESTCATALOG, sourceConfig.getType());
  }

  @Test
  public void testToStringWithoutSecretsDoesNotLeakSecretValues() {
    SourceConfig sourceConfig =
        new SourceConfig()
            .setName("polaris")
            .setType(RESTCATALOG)
            .setConfig(newPolarisLikeConfig().toBytesString());

    String text = connectionReader.toStringWithoutSecrets(sourceConfig);

    assertTrue(text, text.contains("polaris"));
    assertFalse("secret value leaked: " + text, text.contains(TEST_CLIENT_SECRET));
  }

  @Test
  public void testOfficialDocsJsonDeserializes() throws Exception {
    // Shape of the documented POST /api/v3/catalog body for a RESTCATALOG (Polaris) source.
    String json =
        "{"
            + "\"entityType\":\"source\","
            + "\"name\":\"polaris\","
            + "\"type\":\"RESTCATALOG\","
            + "\"config\":{"
            + "\"restEndpointUri\":\"http://localhost:8181/api/catalog\","
            + "\"isUsingVendedCredentials\":false,"
            + "\"allowedNamespaces\":[\"ns1\",\"ns2.child\"],"
            + "\"isRecursiveAllowedNamespaces\":true,"
            + "\"propertyList\":["
            + "{\"name\":\"warehouse\",\"value\":\"test_catalog\"},"
            + "{\"name\":\"scope\",\"value\":\"PRINCIPAL_ROLE:ALL\"},"
            + "{\"name\":\"fs.s3a.endpoint\",\"value\":\"127.0.0.1:9000\"},"
            + "{\"name\":\"fs.s3a.path.style.access\",\"value\":\"true\"}"
            + "],"
            + "\"secretPropertyList\":["
            + "{\"name\":\"credential\",\"value\":\"test-client-id:"
            + TEST_CLIENT_SECRET
            + "\"}"
            + "],"
            + "\"enableAsync\":true,"
            + "\"isCachingEnabled\":true,"
            + "\"maxCacheSpacePct\":100"
            + "},"
            + "\"metadataPolicy\":{\"authTTLMs\":86400000}"
            + "}";

    SourceJsonHolder holder = newRegisteredMapper().readValue(json, SourceJsonHolder.class);

    assertEquals("polaris", holder.getName());
    assertTrue(holder.getConfig() instanceof RestIcebergCatalogPluginConfig);
    RestIcebergCatalogPluginConfig conf = (RestIcebergCatalogPluginConfig) holder.getConfig();
    assertEquals(RESTCATALOG, conf.getType());
    assertEquals("http://localhost:8181/api/catalog", conf.restEndpointUri);
    assertFalse(conf.isUsingVendedCredentials);
    assertEquals(Arrays.asList("ns1", "ns2.child"), conf.allowedNamespaces);
    assertTrue(conf.isRecursiveAllowedNamespaces);
    assertEquals(4, conf.propertyList.size());
    assertEquals(new Property("warehouse", "test_catalog"), conf.propertyList.get(0));
    assertEquals(new Property("scope", "PRINCIPAL_ROLE:ALL"), conf.propertyList.get(1));
    assertEquals(1, conf.secretPropertyList.size());
    assertEquals("credential", conf.secretPropertyList.get(0).name);
    assertTrue(conf.enableAsync);
    assertTrue(conf.isCachingEnabled);
    assertEquals(100, conf.maxCacheSpacePct);
  }

  @Test
  public void testJsonWithVendedCredentialsTrue() throws Exception {
    String json =
        "{\"name\":\"polaris\",\"type\":\"RESTCATALOG\",\"config\":{"
            + "\"restEndpointUri\":\"http://localhost:8181/api/catalog\","
            + "\"isUsingVendedCredentials\":true}}";

    SourceJsonHolder holder = newRegisteredMapper().readValue(json, SourceJsonHolder.class);

    RestIcebergCatalogPluginConfig conf = (RestIcebergCatalogPluginConfig) holder.getConfig();
    assertTrue(conf.isUsingVendedCredentials);
  }

  @Test
  public void testJsonWithoutVendedCredentialsDefaultsToFalse() throws Exception {
    // A pre-26.x / minimal client body that does not know about the vended flag.
    String json =
        "{\"name\":\"rest\",\"type\":\"RESTCATALOG\",\"config\":{"
            + "\"restEndpointUri\":\"http://localhost:8181/api/catalog\"}}";

    SourceJsonHolder holder = newRegisteredMapper().readValue(json, SourceJsonHolder.class);

    RestIcebergCatalogPluginConfig conf = (RestIcebergCatalogPluginConfig) holder.getConfig();
    assertFalse(conf.isUsingVendedCredentials);
    assertTrue(conf.isRecursiveAllowedNamespaces);
    assertTrue(conf.enableAsync);
    assertTrue(conf.isCachingEnabled);
    assertEquals(100, conf.maxCacheSpacePct);
    assertNull(conf.allowedNamespaces);
  }

  @Test
  public void testJsonRoundTripPreservesTypeAndFields() throws Exception {
    ObjectMapper mapper = newRegisteredMapper();
    SourceJsonHolder holder = new SourceJsonHolder();
    holder.setName("polaris");
    RestIcebergCatalogPluginConfig original = newPolarisLikeConfig();
    original.isUsingVendedCredentials = true;
    holder.setConfig(original);

    String json = mapper.writeValueAsString(holder);
    assertTrue(json, json.contains("\"type\":\"RESTCATALOG\""));
    assertTrue(json, json.contains("\"isUsingVendedCredentials\":true"));

    SourceJsonHolder read = mapper.readValue(json, SourceJsonHolder.class);
    assertTrue(read.getConfig() instanceof RestIcebergCatalogPluginConfig);
    assertEquals(original, read.getConfig());
  }
}
