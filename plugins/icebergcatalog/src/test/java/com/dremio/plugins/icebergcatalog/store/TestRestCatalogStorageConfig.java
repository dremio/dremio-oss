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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.SupportsFsCreation;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.plugins.util.AwsCredentialProviderUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.FileIO;
import org.junit.Test;
import org.mockito.MockedStatic;

/**
 * Storage settings of a RESTCATALOG source on the static-credential path (Phase 4 baseline): how
 * the catalog properties and catalog credentials reach the Hadoop configuration of the plugin and,
 * from there, Dremio's S3 file system that reads and writes table data and metadata files.
 *
 * <p>Official Polaris OSS baseline: {@code isUsingVendedCredentials=false}, catalog properties
 * {@code warehouse}, {@code scope}, {@code fs.s3a.aws.credentials.provider=SimpleAWSCredentials
 * Provider}, catalog credentials {@code credential}, {@code fs.s3a.access.key}, {@code
 * fs.s3a.secret.key}. S3-compatible storage (MinIO) adds the endpoint settings below.
 */
public class TestRestCatalogStorageConfig extends BaseTestQuery {

  private static final String PROVIDER_KEY = "fs.s3a.aws.credentials.provider";
  private static final String SIMPLE_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
  private static final String TABLE_LOCATION = "s3://test-bucket/warehouse/ns1/t1";

  // Dummy, test-only values. Assertions check where they go and that they go nowhere else.
  private static final String TEST_CLIENT_SECRET = "test-client-secret-value-4001";
  private static final String TEST_S3_ACCESS = "test-s3-access-value-4002";
  private static final String TEST_S3_SECRET = "test-s3-secret-value-4003";
  private static final String TEST_TOKEN = "test-token-value-4004";

  /** S3-compatible (MinIO) settings, copied verbatim to the Hadoop configuration. */
  private static final List<Property> S3_COMPATIBLE_PROPERTIES =
      Arrays.asList(
          new Property("fs.s3a.endpoint", "127.0.0.1:9000"),
          new Property("fs.s3a.connection.ssl.enabled", "false"),
          new Property("fs.s3a.path.style.access", "true"),
          new Property("dremio.s3.compat", "true"),
          new Property("dremio.bucket.discovery.enabled", "false"),
          new Property("dremio.s3.region", "us-east-1"),
          new Property("fs.s3a.requester.pays.enabled", "false"),
          new Property("fs.s3a.bucket.test-bucket.endpoint", "127.0.0.1:9000"));

  /** REST client settings that must stay out of the Hadoop configuration. */
  private static final List<Property> REST_CLIENT_ONLY_PROPERTIES =
      Arrays.asList(
          new Property("scope", "PRINCIPAL_ROLE:ALL"),
          new Property("oauth2-server-uri", "http://localhost:8181/api/catalog/v1/oauth/tokens"),
          new Property("token-refresh-enabled", "true"),
          new Property("header.X-Custom-Header", "custom-value"),
          new Property("rest.auth.type", "oauth2"),
          new Property("rest.client.socket-timeout-ms", "60000"),
          new Property("rest.access-key-id", "test-rest-access-key-id"));

  private final StoragePluginId storagePluginId = mock(StoragePluginId.class);

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** The official static-credential baseline, optionally with the S3-compatible settings. */
  private static RestIcebergCatalogPluginConfig baselineConfig(boolean s3Compatible) {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.isUsingVendedCredentials = false;
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "test_catalog"),
                new Property("scope", "PRINCIPAL_ROLE:ALL"),
                new Property(PROVIDER_KEY, SIMPLE_PROVIDER)));
    if (s3Compatible) {
      conf.propertyList.addAll(S3_COMPATIBLE_PROPERTIES);
    }
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", "test-client-id:" + TEST_CLIENT_SECRET),
                new Property("fs.s3a.access.key", TEST_S3_ACCESS),
                new Property("fs.s3a.secret.key", TEST_S3_SECRET)));
    return conf;
  }

  private static void removeProperty(List<Property> properties, String name) {
    properties.removeIf(p -> name.equals(p.name));
  }

  private RestIcebergCatalogPlugin newPlugin(RestIcebergCatalogPluginConfig conf) {
    return new RestIcebergCatalogPlugin(conf, getSabotContext(), "polaris", () -> storagePluginId);
  }

  /**
   * Applies what {@code DatasetFileSystemCache} does before it instantiates the file system for an
   * {@code s3://} path: the {@code dremioS3} scheme and the S3 credential provider defaults.
   */
  private static Configuration asDremioS3Conf(Configuration fsConf) throws Exception {
    Configuration conf = new Configuration(fsConf);
    FileSystemConfUtil.initializeConfiguration(
        new URI("dremioS3", "test-bucket", "/test-bucket/warehouse/ns1/t1", null, null), conf);
    return conf;
  }

  private static void assertNoClientCredentialIn(Configuration conf) {
    for (Map.Entry<String, String> entry : conf) {
      String value = entry.getValue();
      assertFalse(
          "client secret copied to " + entry.getKey(),
          value != null && value.contains(TEST_CLIENT_SECRET));
      assertFalse("token copied to " + entry.getKey(), value != null && value.contains(TEST_TOKEN));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Plugin Hadoop configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testBaselineStorageSettingsReachFsConfWithoutContactingCatalog() throws Exception {
    RestIcebergCatalogPlugin plugin = newPlugin(baselineConfig(false));

    try (MockedStatic<CatalogUtil> mockCatalogUtil = mockStatic(CatalogUtil.class)) {
      plugin.start();
      try {
        mockCatalogUtil.verify(() -> CatalogUtil.loadCatalog(any(), any(), any(), any()), never());

        Configuration fsConf = plugin.getFsConfCopy();
        assertEquals(SIMPLE_PROVIDER, fsConf.get(PROVIDER_KEY));
        // Catalog credentials (secretPropertyList) are storage settings as well.
        assertEquals(TEST_S3_ACCESS, fsConf.get("fs.s3a.access.key"));
        assertEquals(TEST_S3_SECRET, fsConf.get("fs.s3a.secret.key"));
        // The OAuth2 settings of the REST client are not.
        assertNull(fsConf.get("credential"));
        assertNull(fsConf.get("scope"));
        assertNoClientCredentialIn(fsConf);
      } finally {
        plugin.close();
      }
    }
  }

  @Test
  public void testRestClientOnlyPropertiesStayOutOfFsConf() {
    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    conf.propertyList.addAll(REST_CLIENT_ONLY_PROPERTIES);
    conf.secretPropertyList.add(new Property("token", TEST_TOKEN));
    // AWS SigV4 signing of REST requests: REST client credentials.
    conf.secretPropertyList.add(new Property("rest.secret-access-key", TEST_TOKEN + "-sigv4"));
    conf.secretPropertyList.add(new Property("rest.session-token", TEST_TOKEN + "-session"));
    conf.isUsingVendedCredentials = true;

    Configuration fsConf = newPlugin(conf).getFsConfCopy();

    for (Property p : REST_CLIENT_ONLY_PROPERTIES) {
      assertNull(p.name, fsConf.get(p.name));
    }
    assertNull(fsConf.get("credential"));
    assertNull(fsConf.get("token"));
    assertNull(fsConf.get("rest.secret-access-key"));
    assertNull(fsConf.get("rest.session-token"));
    assertNull(fsConf.get(RestIcebergCatalogPlugin.ACCESS_DELEGATION_HEADER_PROPERTY));
    assertNoClientCredentialIn(fsConf);
    // Storage settings are unaffected.
    assertEquals(SIMPLE_PROVIDER, fsConf.get(PROVIDER_KEY));
    assertEquals(TEST_S3_SECRET, fsConf.get("fs.s3a.secret.key"));
  }

  @Test
  public void testS3CompatibleSettingsPassThroughVerbatim() {
    Configuration fsConf = newPlugin(baselineConfig(true)).getFsConfCopy();

    for (Property p : S3_COMPATIBLE_PROPERTIES) {
      assertEquals(p.name, p.value, fsConf.get(p.name));
    }
    // Dremio's S3 file system takes the endpoint as host:port; the value is not rewritten.
    assertFalse(fsConf.get("fs.s3a.endpoint").contains("://"));
  }

  @Test
  public void testSecretPropertyListOverridesPlainPropertyList() {
    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    conf.propertyList.add(new Property("fs.s3a.secret.key", "stale-plain-value"));
    conf.propertyList.add(new Property("fs.s3a.endpoint", "plain-host:9000"));
    conf.secretPropertyList.add(new Property("fs.s3a.endpoint", "secret-host:9000"));

    Configuration fsConf = newPlugin(conf).getFsConfCopy();

    assertEquals(TEST_S3_SECRET, fsConf.get("fs.s3a.secret.key"));
    assertEquals("secret-host:9000", fsConf.get("fs.s3a.endpoint"));
  }

  @Test
  public void testVendedCredentialsFlagKeepsSourceLevelStorageSettings() {
    RestIcebergCatalogPluginConfig vended = baselineConfig(true);
    vended.isUsingVendedCredentials = true;

    Configuration staticConf = newPlugin(baselineConfig(true)).getFsConfCopy();
    Configuration vendedConf = newPlugin(vended).getFsConfCopy();

    // The source-level configuration (the base of every table file system) does not depend on the
    // flag; credentials vended for a table, if any, are applied per table on top of it.
    for (String key :
        Arrays.asList(PROVIDER_KEY, "fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.endpoint")) {
      assertEquals(key, staticConf.get(key), vendedConf.get(key));
    }
    assertNull(vendedConf.get(RestIcebergCatalogPlugin.ACCESS_DELEGATION_HEADER_PROPERTY));
  }

  // ---------------------------------------------------------------------------------------------
  // Credential provider
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testExplicitSimpleProviderIsKeptForDremioS3() throws Exception {
    Configuration s3Conf = asDremioS3Conf(newPlugin(baselineConfig(true)).getFsConfCopy());

    assertEquals(S3FileSystem.class.getName(), s3Conf.get(FileSystemConfUtil.FS_DREMIO_S3_IMPL));
    assertEquals(SIMPLE_PROVIDER, s3Conf.get(PROVIDER_KEY));
    assertNotNull(AwsCredentialProviderUtils.getCredentialsProvider(s3Conf));
  }

  /**
   * Hadoop's built-in value of {@code fs.s3a.aws.credentials.provider} (core-default.xml) is a
   * comma-separated provider chain, which Dremio's S3 client rejects ("Invalid
   * AWSCredentialsProvider provided"; live, before the fix, the first S3 access of a source without
   * the property failed). The plugin replaces the built-in chain with SimpleAWSCredentialsProvider,
   * so the access key is used (docs/polaris/storage.md).
   */
  @Test
  public void testMissingProviderUsesTheAccessKey() throws Exception {
    String hadoopDefault = new Configuration().get(PROVIDER_KEY);
    assertNotNull(hadoopDefault);
    assertTrue(hadoopDefault, hadoopDefault.contains(","));
    Configuration chain = new Configuration(false);
    chain.set(PROVIDER_KEY, hadoopDefault);
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> AwsCredentialProviderUtils.getCredentialsProvider(chain));
    assertTrue(e.getMessage(), e.getMessage().contains("Invalid AWSCredentialsProvider"));

    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    removeProperty(conf.propertyList, PROVIDER_KEY);
    RestIcebergCatalogPlugin plugin = newPlugin(conf);

    assertEquals(SIMPLE_PROVIDER, plugin.getFsConfCopy().get(PROVIDER_KEY));
    Configuration s3Conf = asDremioS3Conf(plugin.getFsConfCopy());
    assertEquals(SIMPLE_PROVIDER, s3Conf.get(PROVIDER_KEY));
    assertEquals(TEST_S3_ACCESS, s3Conf.get("fs.s3a.access.key"));
    assertEquals(TEST_S3_SECRET, s3Conf.get("fs.s3a.secret.key"));
    assertNotNull(AwsCredentialProviderUtils.getCredentialsProvider(s3Conf));
  }

  /** Only Hadoop's built-in value is replaced; any other provider is kept. */
  @Test
  public void testOnlyHadoopBuiltInProviderIsReplaced() {
    Configuration builtIn = new Configuration();
    RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider(builtIn);
    assertEquals(SIMPLE_PROVIDER, builtIn.get(PROVIDER_KEY));

    // Dremio copies the fs.* defaults into its file system configurations with set().
    Configuration copiedDefaults = new Configuration(false);
    copiedDefaults.set(PROVIDER_KEY, new Configuration().get(PROVIDER_KEY));
    RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider(copiedDefaults);
    assertEquals(SIMPLE_PROVIDER, copiedDefaults.get(PROVIDER_KEY));

    // E.g. set in core-site.xml: not Hadoop's built-in default.
    Configuration site = new Configuration();
    site.set(PROVIDER_KEY, "com.amazonaws.auth.InstanceProfileCredentialsProvider");
    RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider(site);
    assertEquals("com.amazonaws.auth.InstanceProfileCredentialsProvider", site.get(PROVIDER_KEY));

    Configuration empty = new Configuration(false);
    empty.set(PROVIDER_KEY, "");
    RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider(empty);
    assertEquals("", empty.get(PROVIDER_KEY));

    Configuration none = new Configuration(false);
    RestIcebergCatalogPlugin.replaceHadoopDefaultCredentialsProvider(none);
    assertNull(none.get(PROVIDER_KEY));
  }

  /**
   * A source without S3 credentials (no key, no provider) fails on both S3 clients instead of
   * falling back to the Dremio host's identity. Hadoop's built-in chain would not fail closed:
   * Hadoop S3A walks it to the {@code AWS_*} environment variables and the EC2 instance profile
   * (live: "Unable to load AWS credentials from environment variables"), and an empty provider
   * makes FileSystemConfUtil derive the same. Using the host's identity needs an explicit provider
   * (or an explicit empty value) on the source.
   */
  @Test
  public void testSourceWithoutS3CredentialsDoesNotFallBackToTheHostIdentity() throws Exception {
    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    removeProperty(conf.propertyList, PROVIDER_KEY);
    removeProperty(conf.secretPropertyList, "fs.s3a.access.key");
    removeProperty(conf.secretPropertyList, "fs.s3a.secret.key");

    Configuration s3Conf = asDremioS3Conf(newPlugin(conf).getFsConfCopy());

    assertEquals(SIMPLE_PROVIDER, s3Conf.get(PROVIDER_KEY));
    assertNull(s3Conf.get("fs.s3a.access.key"));
    // Dremio's S3 client (AWS SDK v2).
    assertThrows(
        RuntimeException.class,
        () -> AwsCredentialProviderUtils.getCredentialsProvider(s3Conf).resolveCredentials());
    // Hadoop S3A (AWS SDK v1).
    assertThrows(
        RuntimeException.class,
        () ->
            new org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider(
                    new URI("s3a://test-bucket/"), s3Conf)
                .getCredentials());
  }

  @Test
  public void testOwnS3Credentials() {
    Configuration conf = new Configuration();
    assertFalse("Hadoop's built-in chain", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.set(PROVIDER_KEY, SIMPLE_PROVIDER);
    assertFalse(
        "key-based provider without key", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.set(PROVIDER_KEY, "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
    assertFalse(
        "key-based provider without key", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.set(PROVIDER_KEY, "");
    assertFalse("empty provider", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.set(PROVIDER_KEY, "com.amazonaws.auth.InstanceProfileCredentialsProvider");
    assertTrue("keyless provider", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.set(PROVIDER_KEY, SIMPLE_PROVIDER);
    conf.set("fs.s3a.bucket.test-bucket.access.key", TEST_S3_ACCESS);
    assertTrue("per-bucket key", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
    conf.unset("fs.s3a.bucket.test-bucket.access.key");
    conf.set("fs.s3a.access.key", TEST_S3_ACCESS);
    assertTrue("access key", RestIcebergCatalogPlugin.hasOwnS3Credentials(conf));
  }

  /** A provider set on the source wins over the derived one. */
  @Test
  public void testSourceProviderIsKept() {
    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    removeProperty(conf.propertyList, PROVIDER_KEY);
    conf.propertyList.add(
        new Property(PROVIDER_KEY, "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"));

    assertEquals(
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        newPlugin(conf).getFsConfCopy().get(PROVIDER_KEY));
  }

  /** A blank provider lets FileSystemConfUtil derive SimpleAWSCredentialsProvider from the keys. */
  @Test
  public void testBlankProviderIsDerivedFromAccessKey() throws Exception {
    RestIcebergCatalogPluginConfig conf = baselineConfig(true);
    removeProperty(conf.propertyList, PROVIDER_KEY);
    conf.propertyList.add(new Property(PROVIDER_KEY, ""));

    Configuration s3Conf = asDremioS3Conf(newPlugin(conf).getFsConfCopy());

    assertEquals(SIMPLE_PROVIDER, s3Conf.get(PROVIDER_KEY));
    assertEquals(TEST_S3_ACCESS, s3Conf.get("fs.s3a.access.key"));
    assertEquals(TEST_S3_SECRET, s3Conf.get("fs.s3a.secret.key"));
  }

  // ---------------------------------------------------------------------------------------------
  // Plugin file system -> DremioFileIO (data and metadata files)
  // ---------------------------------------------------------------------------------------------

  /**
   * The file system the plugin creates for a table location is Dremio's S3 file system, built from
   * the plugin configuration (no REST catalog involved), and DremioFileIO wraps it. Compatibility
   * mode and disabled bucket discovery keep initialization local: no STS or ListBuckets call.
   */
  @Test
  public void testTableLocationFileSystemIsDremioS3WithSourceSettings() throws Exception {
    RestIcebergCatalogPlugin plugin = newPlugin(baselineConfig(true));
    List<String> dataset = Arrays.asList("polaris", "ns1", "t1");

    try (MockedStatic<CatalogUtil> mockCatalogUtil = mockStatic(CatalogUtil.class)) {
      plugin.start();
      try {
        FileSystem fs =
            plugin.createFS(
                SupportsFsCreation.builder()
                    .filePath(TABLE_LOCATION)
                    .withSystemUserName()
                    .withSystemUserId()
                    .dataset(dataset));
        org.apache.hadoop.fs.FileSystem hadoopFs = fs.unwrap(org.apache.hadoop.fs.FileSystem.class);
        assertTrue(hadoopFs.getClass().getName(), hadoopFs instanceof S3FileSystem);

        Configuration s3Conf = hadoopFs.getConf();
        for (Property p : S3_COMPATIBLE_PROPERTIES) {
          assertEquals(p.name, p.value, s3Conf.get(p.name));
        }
        assertEquals(SIMPLE_PROVIDER, s3Conf.get(PROVIDER_KEY));
        assertEquals(TEST_S3_ACCESS, s3Conf.get("fs.s3a.access.key"));
        assertEquals(TEST_S3_SECRET, s3Conf.get("fs.s3a.secret.key"));
        assertNull(s3Conf.get("credential"));
        assertNull(s3Conf.get("scope"));
        assertNoClientCredentialIn(s3Conf);

        FileIO fileIO = plugin.createIcebergFileIO(fs, null, dataset, null, null);
        assertTrue(fileIO instanceof DremioFileIO);
        mockCatalogUtil.verify(() -> CatalogUtil.loadCatalog(any(), any(), any(), any()), never());
      } finally {
        plugin.close();
      }
    }
  }
}
