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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.hive.exec.FileSystemConfUtil;
import com.dremio.options.OptionManager;
import com.dremio.plugins.s3.store.S3ClientProperties;
import com.dremio.plugins.s3.store.S3ClientProperties.PropertyName;
import com.dremio.plugins.util.S3PluginUtils;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

/**
 * S3-compatible storage (MinIO and other non-AWS endpoints) on a RESTCATALOG source: how the custom
 * endpoint, path-style, SSL, region and requester-pays catalog properties travel from the source to
 * the configuration of Dremio's S3 file system ({@code dremioS3}), which reads and writes table
 * files. No network access: the plugin is built on mocks and never started.
 *
 * <p>Dremio's S3 file system talks to S3 through two clients. The S3A file system (AWS SDK v1)
 * reads only {@code fs.s3a.*} keys. The SDK v2 clients (async reads, bucket checks) resolve their
 * settings through {@link S3ClientProperties#PROPERTY_NAMES}, which also accepts some Iceberg and
 * Dremio keys. A setting therefore has to be given under the {@code fs.s3a.*} key to reach both.
 */
public class TestRestCatalogS3CompatibleProps {

  private static final String BUCKET = "warehouse-bucket";
  private static final String OTHER_BUCKET = "other-bucket";

  // Dummy, test-only values.
  private static final String TEST_CLIENT_CREDENTIAL = "test-client-id:test-client-secret-5001";
  private static final String TEST_S3_ACCESS = "test-s3-access-value-5002";
  private static final String TEST_S3_SECRET = "test-s3-secret-value-5003";

  private static final String ENDPOINT = "fs.s3a.endpoint";
  private static final String SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
  private static final String PATH_STYLE = "fs.s3a.path.style.access";
  private static final String ENDPOINT_REGION = "fs.s3a.endpoint.region";
  private static final String REQUESTER_PAYS = "fs.s3a.requester.pays.enabled";
  private static final String COMPAT = "dremio.s3.compat";
  private static final String BUCKET_DISCOVERY = "dremio.bucket.discovery.enabled";
  private static final String DREMIO_REGION = "dremio.s3.region";
  private static final String PROVIDER = "fs.s3a.aws.credentials.provider";
  private static final String SIMPLE_PROVIDER =
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";

  private PluginSabotContext sabotContext;

  @Before
  public void setUp() {
    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_S3_V2_CLIENT)).thenReturn(true);
    sabotContext = mock(PluginSabotContext.class);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** A source with static S3 keys and the given storage properties (no other storage settings). */
  private RestIcebergCatalogPlugin newPlugin(Property... storageProperties) {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.isUsingVendedCredentials = false;
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "test_catalog"),
                new Property("scope", "PRINCIPAL_ROLE:ALL"),
                new Property(PROVIDER, SIMPLE_PROVIDER)));
    conf.propertyList.addAll(Arrays.asList(storageProperties));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", TEST_CLIENT_CREDENTIAL),
                new Property("fs.s3a.access.key", TEST_S3_ACCESS),
                new Property("fs.s3a.secret.key", TEST_S3_SECRET)));
    return new RestIcebergCatalogPlugin(
        conf, sabotContext, "minio_src", () -> mock(StoragePluginId.class));
  }

  /** The MinIO recipe that passed the live checks (plain HTTP, endpoint as host:port). */
  private static Property[] minioRecipe(String endpoint) {
    return new Property[] {
      new Property(ENDPOINT, endpoint),
      new Property(SSL_ENABLED, "false"),
      new Property(PATH_STYLE, "true"),
      new Property(COMPAT, "true"),
    };
  }

  /**
   * The configuration Dremio's S3 file system receives for a table in {@code bucket}: what {@code
   * DatasetFileSystemCache} does with a fresh plugin configuration before it creates the file
   * system for an {@code s3://bucket/...} location.
   */
  private static Configuration dremioS3Conf(RestIcebergCatalogPlugin plugin, String bucket)
      throws Exception {
    Configuration conf = plugin.getFsConfCopy();
    FileSystemConfUtil.initializeConfiguration(
        new URI("dremioS3", bucket, "/" + bucket + "/ns1/t1/metadata/v1.metadata.json", null, null),
        conf);
    return conf;
  }

  /**
   * The key the SDK v2 clients of Dremio's S3 file system read for {@code property}, or null if
   * none is set: like {@code S3ClientProperties.validateAndGetPropertyNameToUse}, the first key of
   * {@link S3ClientProperties#PROPERTY_NAMES} with a non-empty value.
   */
  private static String keyUsedBySdkV2Clients(Configuration conf, PropertyName property) {
    for (String key : S3ClientProperties.PROPERTY_NAMES.get(property)) {
      String value = conf.getTrimmed(key);
      if (value != null && !value.isEmpty()) {
        return key;
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------------------------
  // Custom endpoint
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testMinioRecipeReachesDremioS3Conf() throws Exception {
    RestIcebergCatalogPlugin plugin = newPlugin(minioRecipe("127.0.0.1:9000"));

    Configuration conf = dremioS3Conf(plugin, BUCKET);

    assertThat(conf.get(FileSystemConfUtil.FS_DREMIO_S3_IMPL))
        .isEqualTo("com.dremio.plugins.s3.store.S3FileSystem");
    assertThat(conf.get(ENDPOINT)).isEqualTo("127.0.0.1:9000");
    assertThat(conf.get(SSL_ENABLED)).isEqualTo("false");
    assertThat(conf.get(PATH_STYLE)).isEqualTo("true");
    assertThat(conf.get(COMPAT)).isEqualTo("true");
    assertThat(conf.get(PROVIDER)).isEqualTo(SIMPLE_PROVIDER);
    assertThat(conf.get("fs.s3a.access.key")).isEqualTo(TEST_S3_ACCESS);
    assertThat(conf.get("fs.s3a.secret.key")).isEqualTo(TEST_S3_SECRET);
    // The SDK v2 clients read the same fs.s3a keys.
    assertThat(keyUsedBySdkV2Clients(conf, PropertyName.ENDPOINT_OVERRIDE)).isEqualTo(ENDPOINT);
    assertThat(keyUsedBySdkV2Clients(conf, PropertyName.SECURE_CONNECTIONS)).isEqualTo(SSL_ENABLED);
    assertThat(keyUsedBySdkV2Clients(conf, PropertyName.PATH_STYLE_ACCESS)).isEqualTo(PATH_STYLE);
    // The REST client credential never reaches the file system configuration.
    assertThat(conf.get("credential")).isNull();
  }

  /**
   * The plugin copies the endpoint verbatim. Dremio's S3 file system used to add a second scheme to
   * an endpoint that has one ({@code http://http://host:port}, live: "Unable to execute HTTP
   * request: http"); since Phase 4 it keeps it (TestS3FileSystem#testGetEndpointKeepsScheme).
   * {@code host:port} with {@code fs.s3a.connection.ssl.enabled} remains the documented form.
   */
  @Test
  public void testEndpointIsPassedVerbatimIncludingAScheme() throws Exception {
    Configuration hostPort = dremioS3Conf(newPlugin(minioRecipe("minio.local:9000")), BUCKET);
    Configuration withScheme =
        dremioS3Conf(newPlugin(minioRecipe("http://minio.local:9000")), BUCKET);
    Configuration tls =
        dremioS3Conf(
            newPlugin(
                new Property(ENDPOINT, "minio.local:9443"),
                new Property(SSL_ENABLED, "true"),
                new Property(COMPAT, "true")),
            BUCKET);

    assertThat(hostPort.get(ENDPOINT)).isEqualTo("minio.local:9000");
    assertThat(withScheme.get(ENDPOINT)).isEqualTo("http://minio.local:9000");
    assertThat(tls.get(ENDPOINT)).isEqualTo("minio.local:9443");
    assertThat(tls.get(SSL_ENABLED)).isEqualTo("true");
  }

  /**
   * Iceberg FileIO keys ({@code s3.endpoint}, {@code s3.path-style-access}) are copied too, but
   * only the SDK v2 clients read them: the S3A client still uses the AWS endpoint. The live check
   * with only these keys reached AWS S3 ("The AWS Access Key Id you provided does not exist"). The
   * path-style key does not even reach the SDK v2 clients: Hadoop's default for {@code
   * fs.s3a.path.style.access} ({@code false}) is always present and takes precedence.
   */
  @Test
  public void testIcebergFileIoKeysDoNotConfigureTheS3aClient() throws Exception {
    RestIcebergCatalogPlugin plugin =
        newPlugin(
            new Property("s3.endpoint", "http://127.0.0.1:9000"),
            new Property("s3.path-style-access", "true"),
            new Property(COMPAT, "true"));

    Configuration conf = dremioS3Conf(plugin, BUCKET);

    assertThat(conf.get(ENDPOINT)).isNull();
    assertThat(conf.get("s3.endpoint")).isEqualTo("http://127.0.0.1:9000");
    assertThat(keyUsedBySdkV2Clients(conf, PropertyName.ENDPOINT_OVERRIDE))
        .isEqualTo("s3.endpoint");
    assertThat(conf.get("s3.path-style-access")).isEqualTo("true");
    assertThat(keyUsedBySdkV2Clients(conf, PropertyName.PATH_STYLE_ACCESS)).isEqualTo(PATH_STYLE);
    assertThat(conf.getBoolean(PATH_STYLE, true)).isFalse();
  }

  /**
   * Bucket-level keys ({@code fs.s3a.bucket.<bucket>.*}) are applied only to the file system of
   * that bucket; every file system starts from a fresh copy of the plugin configuration.
   */
  @Test
  public void testBucketLevelEndpointAndSslApplyOnlyToThatBucket() throws Exception {
    RestIcebergCatalogPlugin plugin =
        newPlugin(
            new Property(ENDPOINT, "minio-a:9000"),
            new Property(SSL_ENABLED, "false"),
            new Property(COMPAT, "true"),
            new Property("fs.s3a.bucket." + OTHER_BUCKET + ".endpoint", "minio-b:9443"),
            new Property("fs.s3a.bucket." + OTHER_BUCKET + ".connection.ssl.enabled", "true"));

    Configuration warehouse = dremioS3Conf(plugin, BUCKET);
    Configuration other = dremioS3Conf(plugin, OTHER_BUCKET);

    assertThat(warehouse.get(ENDPOINT)).isEqualTo("minio-a:9000");
    assertThat(warehouse.get(SSL_ENABLED)).isEqualTo("false");
    assertThat(other.get(ENDPOINT)).isEqualTo("minio-b:9443");
    assertThat(other.get(SSL_ENABLED)).isEqualTo("true");
    assertThat(plugin.getFsConfCopy().get(ENDPOINT)).isEqualTo("minio-a:9000");
  }

  // ---------------------------------------------------------------------------------------------
  // Path-style, compatibility mode, bucket discovery
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testPathStyleCompatAndBucketDiscoveryFlagsPassThrough() throws Exception {
    Configuration defaults = dremioS3Conf(newPlugin(new Property(ENDPOINT, "minio:9000")), BUCKET);
    Configuration explicit =
        dremioS3Conf(
            newPlugin(
                new Property(ENDPOINT, "minio:9000"),
                new Property(PATH_STYLE, "true"),
                new Property(COMPAT, "true"),
                new Property(BUCKET_DISCOVERY, "false")),
            BUCKET);

    // Without the properties: Hadoop's path-style default (false); compatibility mode and bucket
    // discovery are unset, i.e. off and on in S3FileSystem.
    assertThat(defaults.get(PATH_STYLE)).isEqualTo("false");
    assertThat(defaults.get(COMPAT)).isNull();
    assertThat(defaults.get(BUCKET_DISCOVERY)).isNull();
    assertThat(explicit.get(PATH_STYLE)).isEqualTo("true");
    assertThat(explicit.get(COMPAT)).isEqualTo("true");
    assertThat(explicit.get(BUCKET_DISCOVERY)).isEqualTo("false");
  }

  // ---------------------------------------------------------------------------------------------
  // Region
  // ---------------------------------------------------------------------------------------------

  /**
   * {@code fs.s3a.endpoint.region} is read by both clients; {@code dremio.s3.region} only by the
   * SDK v2 clients, where it wins. Against a MinIO with a non-default region only {@code
   * fs.s3a.endpoint.region} passed the live check.
   */
  @Test
  public void testRegionKeysAndTheirPrecedence() throws Exception {
    Configuration none = dremioS3Conf(newPlugin(minioRecipe("minio:9000")), BUCKET);
    Configuration endpointRegion =
        dremioS3Conf(
            newPlugin(
                new Property(ENDPOINT, "minio:9000"),
                new Property(ENDPOINT_REGION, "ap-northeast-2")),
            BUCKET);
    Configuration both =
        dremioS3Conf(
            newPlugin(
                new Property(ENDPOINT, "minio:9000"),
                new Property(DREMIO_REGION, "us-east-1"),
                new Property(ENDPOINT_REGION, "ap-northeast-2")),
            BUCKET);

    assertThat(keyUsedBySdkV2Clients(none, PropertyName.REGION_OVERRIDE)).isNull();
    assertThat(keyUsedBySdkV2Clients(endpointRegion, PropertyName.REGION_OVERRIDE))
        .isEqualTo(ENDPOINT_REGION);
    assertThat(endpointRegion.get(ENDPOINT_REGION)).isEqualTo("ap-northeast-2");
    assertThat(keyUsedBySdkV2Clients(both, PropertyName.REGION_OVERRIDE)).isEqualTo(DREMIO_REGION);
    assertThat(both.get(ENDPOINT_REGION)).isEqualTo("ap-northeast-2");
  }

  /**
   * Dremio's SDK v2 clients accept only AWS region ids, so a MinIO configured with a custom region
   * name cannot be read ("minio-local is not a valid AWS region.").
   */
  @Test
  public void testOnlyAwsRegionIdsAreAccepted() {
    assertThat(S3PluginUtils.isValidAwsRegion("us-east-1")).isTrue();
    assertThat(S3PluginUtils.isValidAwsRegion("ap-northeast-2")).isTrue();
    assertThat(S3PluginUtils.isValidAwsRegion("minio-local")).isFalse();
    assertThat(S3PluginUtils.isValidAwsRegion("")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // Requester pays
  // ---------------------------------------------------------------------------------------------

  /**
   * Requester pays is on unless the source turns it off: the SDK v2 clients then send {@code
   * x-amz-request-payer: requester}. MinIO ignores the header (live check: reads and writes pass
   * with and without {@code fs.s3a.requester.pays.enabled=false}).
   */
  @Test
  public void testRequesterPaysIsOnUnlessTurnedOff() throws Exception {
    Configuration unset = dremioS3Conf(newPlugin(minioRecipe("minio:9000")), BUCKET);
    Configuration off =
        dremioS3Conf(
            newPlugin(new Property(ENDPOINT, "minio:9000"), new Property(REQUESTER_PAYS, "false")),
            BUCKET);

    assertThat(S3ClientProperties.REQUESTER_PAYS_DEFAULT).isTrue();
    assertThat(keyUsedBySdkV2Clients(unset, PropertyName.REQUESTER_PAYS)).isNull();
    assertThat(keyUsedBySdkV2Clients(off, PropertyName.REQUESTER_PAYS)).isEqualTo(REQUESTER_PAYS);
    assertThat(off.getBoolean(REQUESTER_PAYS, true)).isFalse();
  }

  // ---------------------------------------------------------------------------------------------
  // Overrides of the plugin's S3 defaults
  // ---------------------------------------------------------------------------------------------

  /**
   * Source properties win over the S3A defaults the plugin sets ({@link
   * FileSystemConfUtil#S3_PROPS}), e.g. to fail fast on a wrong endpoint: with the default retries
   * the live check with an unreachable endpoint took about 8 minutes to fail a query.
   */
  @Test
  public void testSourcePropertiesOverridePluginS3Defaults() throws Exception {
    List<Property> overrides =
        Arrays.asList(
            new Property("fs.s3a.connection.maximum", "64"),
            new Property("fs.s3a.attempts.maximum", "1"),
            new Property("fs.s3a.retry.limit", "1"));

    Configuration defaults = dremioS3Conf(newPlugin(minioRecipe("minio:9000")), BUCKET);
    Configuration overridden = dremioS3Conf(newPlugin(overrides.toArray(new Property[0])), BUCKET);

    assertThat(defaults.get("fs.s3a.connection.maximum"))
        .isEqualTo(FileSystemConfUtil.S3_PROPS.get("fs.s3a.connection.maximum"));
    for (Property p : overrides) {
      assertThat(overridden.get(p.name)).as(p.name).isEqualTo(p.value);
    }
  }
}
