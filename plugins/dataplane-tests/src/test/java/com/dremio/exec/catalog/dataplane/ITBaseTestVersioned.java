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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL;
import static com.dremio.exec.ExecConstants.ENABLE_USE_VERSION_SYNTAX;
import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.exec.catalog.CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.ALTERNATIVE_BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.server.BaseTestServerJunit5;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.options.OptionValue;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.findify.s3mock.S3Mock;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;
import software.amazon.awssdk.regions.Region;

@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")
public abstract class ITBaseTestVersioned extends BaseTestServerJunit5 {
  private static AmazonS3 s3Client;
  private static S3Mock s3Mock;
  private static int s3Port;

  @NessieBaseUri private static URI nessieUri;

  private static NessieApiV1 nessieClient;
  private static DataplanePlugin dataplanePlugin;

  private static Catalog catalog;
  private static NamespaceService namespaceService;

  @BeforeAll
  public static void nessieSourceSetup() throws Exception {
    setUpS3Mock();
    setUpNessie();
    setUpDataplanePlugin();
  }

  @AfterAll
  public static void nessieSourceCleanUp() throws Exception {
    AutoCloseables.close(nessieClient);
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  protected static void setUpS3Mock() throws IOException {
    Preconditions.checkState(s3Mock == null);

    // We use S3Mock's in-memory backend implementation to avoid incompatibility issues between
    // Hadoop's S3's implementation
    // and S3Mock's filesystem backend. When doing file deletions, Hadoop executes a
    // "maybeCreateFakeParentDirectory"
    // operation that tries to write a 0 byte object to S3. S3Mock's filesystem backend throws an
    // AmazonS3Exception
    // with a "Is a directory" message. The in-memory backend does not have the same issue.
    // We encountered this problem (in tests only, not AWS S3) when cleaning up Iceberg metadata
    // files after a failed Nessie commit.
    s3Mock = new S3Mock.Builder().withPort(0).withInMemoryBackend().build();

    s3Port = s3Mock.start().localAddress().getPort();

    AwsClientBuilder.EndpointConfiguration endpoint =
        new AwsClientBuilder.EndpointConfiguration(
            String.format("http://localhost:%d", s3Port), Region.US_EAST_1.toString());

    s3Client =
        AmazonS3ClientBuilder.standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(endpoint)
            .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
            .build();

    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(ALTERNATIVE_BUCKET_NAME);
  }

  protected static void setUpNessie() {
    nessieClient =
        HttpClientBuilder.builder().withUri(createNessieURIString()).build(NessieApiV2.class);
  }

  private static NessiePluginConfig prepareConnectionConf(String bucket) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = createNessieURIString();
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = SecretRef.of("bar"); // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = bucket;

    // S3Mock settings
    nessiePluginConfig.propertyList =
        Arrays.asList(
            new Property("fs.s3a.endpoint", "localhost:" + s3Port),
            new Property("fs.s3a.path.style.access", "true"),
            new Property("fs.s3a.connection.ssl.enabled", "false"),
            new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));

    return nessiePluginConfig;
  }

  protected static void setUpDataplanePlugin() {
    getSabotContext()
        .getOptionManager()
        .setOption(OptionValue.createBoolean(SYSTEM, VERSIONED_VIEW_ENABLED.getOptionName(), true));
    getSabotContext()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(SYSTEM, ENABLE_USE_VERSION_SYNTAX.getOptionName(), true));
    getSabotContext()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(SYSTEM, ENABLE_ICEBERG_TIME_TRAVEL.getOptionName(), true));
    getSabotContext()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(
                SYSTEM, REFLECTION_VERSIONED_SOURCE_ENABLED.getOptionName(), true));
    getSabotContext()
        .getOptionManager()
        .setOption(OptionValue.createBoolean(SYSTEM, NESSIE_PLUGIN_ENABLED.getOptionName(), true));

    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();

    SourceConfig sourceConfig =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(DATAPLANE_PLUGIN_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
    dataplanePlugin = catalogImpl.getSystemUserCatalog().getSource(DATAPLANE_PLUGIN_NAME);
    catalog = catalogImpl.getSystemUserCatalog();

    SourceConfig sourceConfigForReflectionTest =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalogImpl.getSystemUserCatalog().createSource(sourceConfigForReflectionTest);

    namespaceService = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
  }

  public static NessieApiV1 getNessieClient() {
    return nessieClient;
  }

  public static DataplanePlugin getDataplanePlugin() {
    return dataplanePlugin;
  }

  public static Catalog getCatalog() {
    return catalog;
  }

  public static Catalog getContextualizedCatalog(String pluginName, VersionContext versionContext) {
    final Catalog resetCatalog =
        getCatalog().resolveCatalogResetContext(DATAPLANE_PLUGIN_NAME, versionContext);
    final Catalog newCatalog =
        resetCatalog.resolveCatalog(ImmutableMap.of(DATAPLANE_PLUGIN_NAME, versionContext));
    return newCatalog;
  }

  public String getContentId(List<String> tableKey, TableVersionContext tableVersionContext) {
    try {
      final VersionedDatasetId versionedDatasetId =
          VersionedDatasetId.fromString(getVersionedDatatsetId(tableKey, tableVersionContext));

      return (versionedDatasetId == null ? null : versionedDatasetId.getContentId());
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public String getVersionedDatatsetId(
      List<String> tableKey, TableVersionContext tableVersionContext) {
    final Catalog contextualizedCatalog =
        getContextualizedCatalog(DATAPLANE_PLUGIN_NAME, tableVersionContext.asVersionContext());

    try {
      return contextualizedCatalog
          .getTableForQuery(
              new NamespaceKey(
                  PathUtils.parseFullPath(
                      fullyQualifiedTableName(DATAPLANE_PLUGIN_NAME, tableKey))))
          .getDatasetConfig()
          .getId()
          .getId();
    } catch (ReferenceNotFoundException r) {
      return null;
    }
  }

  /* Helper function used to set up the NessieURI as a string. */
  private static String createNessieURIString() {
    return nessieUri.resolve("api/v2").toString();
  }

  /* Helper function to create a set of folders in a given a fully qualified table path of format:
   folder1/folder2/.../table1
  */
  protected static void createFoldersForTablePath(
      String sourcePlugin,
      List<String> tablePath,
      VersionContext versionContext,
      String sessionId) {
    // Iterate to get the parent folders where the table should be created (till tableName). Last
    // element is tableName
    List<String> folderPath = tablePath.subList(1, tablePath.size() - 1);
    String query = createFolderAtQueryWithIfNotExists(sourcePlugin, folderPath, versionContext);
    if (StringUtils.isEmpty(sessionId)) {
      runQuery(query);
    } else {
      runQueryInSession(query, sessionId);
    }
  }
}
