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
import static com.dremio.exec.catalog.CatalogOptions.REFLECTION_ARCTIC_ENABLED;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.ALTERNATIVE_BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.fullyQualifiedTableName;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_PLUGIN_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.PathUtils;
import com.dremio.dac.server.BaseTestServerJunit5;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedDatasetId;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
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

@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")  //PERSIST is the new model in Nessie Server
public class ITBaseTestVersioned extends BaseTestServerJunit5 {
  private static S3Mock s3Mock;
  private static int S3_PORT;

  @TempDir static File temporaryDirectory;
  private static Path bucketPath;

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;

  @NessieBaseUri private static URI nessieUri;

  private static NessieApiV1 nessieClient;
  private static DataplanePlugin dataplanePlugin;

  private static Catalog catalog;
  private static NamespaceService namespaceService;

  @BeforeAll
  public static void arcticSetup() throws Exception {
    setUpS3Mock();
    setUpNessie();
    setUpDataplanePlugin();
  }

  @AfterAll
  public static void arcticCleanUp() throws Exception {
    AutoCloseables.close(
        dataplanePlugin,
        nessieClient);
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  protected static void setUpS3Mock() throws IOException {
    bucketPath = Paths.get(temporaryDirectory.getAbsolutePath(), BUCKET_NAME);
    Files.createDirectory(bucketPath);
    Files.createDirectory(Paths.get(temporaryDirectory.getAbsolutePath(), ALTERNATIVE_BUCKET_NAME));

    Preconditions.checkState(s3Mock == null);
    s3Mock =
        new S3Mock.Builder()
            .withPort(0)
            .withFileBackend(temporaryDirectory.getAbsolutePath())
            .build();
    S3_PORT = s3Mock.start().localAddress().getPort();
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
    nessiePluginConfig.awsAccessSecret = "bar"; // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = bucket;

    // S3Mock settings
    nessiePluginConfig.propertyList =
        Arrays.asList(
            new Property("fs.s3a.endpoint", "localhost:" + S3_PORT),
            new Property("fs.s3a.path.style.access", "true"),
            new Property("fs.s3a.connection.ssl.enabled", "false"),
            new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));

    return nessiePluginConfig;
  }

  protected static void setUpDataplanePlugin() {
    getSabotContext()
        .getOptionManager()
        .setOption(
            OptionValue.createBoolean(SYSTEM, DATAPLANE_PLUGIN_ENABLED.getOptionName(), true));
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
            OptionValue.createBoolean(SYSTEM, REFLECTION_ARCTIC_ENABLED.getOptionName(), true));
    getSabotContext()
      .getOptionManager()
      .setOption(
        OptionValue.createBoolean(SYSTEM, NESSIE_PLUGIN_ENABLED.getOptionName(), true));

    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();

    SourceConfig sourceConfig =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(DATAPLANE_PLUGIN_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
    catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
    dataplanePlugin = catalogImpl.getSystemUserCatalog().getSource(DATAPLANE_PLUGIN_NAME);
    catalog = catalogImpl.getSystemUserCatalog();

    SourceConfig sourceConfigForReflectionTest =
      new SourceConfig()
        .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
        .setName(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY_WITH_AUTO_PROMOTE);
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

  public static Path getBucketPath() {
    return bucketPath;
  }

  public static void reinit() {
    databaseAdapter.eraseRepo();
    s3Mock.stop();
    s3Mock.start();
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
    return nessieUri.toString() + "v2";
  }

  /* Helper function to create a set of folders in a DATAPLANE_PLUGIN  given a table path of format:
   folder1/folder2/.../table1
  */
  protected void createFolders(List<String> tablePath, VersionContext versionContext, String sessionId) {
    //Iterate to get the parent folders where the table should be created (till tableName). Last element is tableName
    StringBuilder folderName = new StringBuilder();
    folderName.append(DATAPLANE_PLUGIN_NAME);
    for (int i = 0; i < tablePath.size() - 1; i++) {
      folderName.append(".").append(tablePath.get(i));
      String query = createFolderAtQueryWithIfNotExists(Collections.singletonList(folderName.toString()), versionContext);
      if (StringUtils.isEmpty(sessionId)) {
        runQuery(query);
      } else {
        runQueryInSession(query, sessionId);
      }
    }
  }
}
