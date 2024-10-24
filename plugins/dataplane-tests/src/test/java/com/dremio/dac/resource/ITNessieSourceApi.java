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

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.ALTERNATIVE_BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import com.dremio.common.AutoCloseables;
import com.dremio.dac.server.BaseTestServerJunit5;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.store.CatalogService;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.google.common.base.Preconditions;
import io.findify.s3mock.S3Mock;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.client.ext.NessieClientCustomizer;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientResolver;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.tests.AbstractRelativeReferences;
import org.projectnessie.jaxrs.tests.BaseTestNessieRest;

public class ITNessieSourceApi extends BaseTestServerJunit5 {

  @TempDir static File temporaryDirectory;
  private static int S3_PORT;
  private static S3Mock s3Mock;
  private static Path bucketPath;
  private static NessieApiV2 nessieClient;
  private static DataplanePlugin dataplanePlugin;
  private static Catalog catalog;
  private static NamespaceService namespaceService;

  private static String createNessieURIString() {
    String baseUrl =
        Preconditions.checkNotNull(
            System.getProperty("nessie.server.url"),
            "The nessie.server.url system property must be set");
    return baseUrl + "/api/v2";
  }

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
        NessieClientBuilder.createClientBuilder("HTTP", null)
            .withUri(createNessieURIString())
            .fromConfig(Collections.singletonMap("nessie.force-url-connection-client", "true")::get)
            .build(NessieApiV2.class);
  }

  protected static void setUpDataplanePlugin() {
    SourceConfig sourceConfig =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(DATAPLANE_PLUGIN_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);

    catalog = getCatalogService().getSystemUserCatalog();
    catalog.createSource(sourceConfig);
    dataplanePlugin = catalog.getSource(DATAPLANE_PLUGIN_NAME);

    namespaceService = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
  }

  private static NessiePluginConfig prepareConnectionConf(String bucket) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = createNessieURIString();
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.credentialType =
        AWSAuthenticationType.ACCESS_KEY; // Unused, just needs to be set
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = SecretRef.of("bar"); // Unused, just needs to be set
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

  @Test
  public void testWrongAPI() {
    expectStatus(
        NOT_FOUND,
        getBuilder(
                getHttpClient()
                    .getNessieProxy()
                    .path(String.format("/source/%s/treez", DATAPLANE_PLUGIN_NAME)))
            .buildGet());
  }

  @Test
  public void testInvalidSource() {
    expectStatus(
        NOT_FOUND,
        getBuilder(
                getHttpClient()
                    .getNessieProxy()
                    .path(String.format("/v2/source/%s/trees", "invalidSource")))
            .buildGet());
  }

  @Nested
  @NessieApiVersions(versions = NessieApiVersion.V2)
  class NessieRestApi extends BaseTestNessieRest {

    /** Workaround to obtain proper test display names in surefire reports. */
    @Nested
    @NessieApiVersions(versions = NessieApiVersion.V2)
    public class RelativeReferences extends AbstractRelativeReferences {
      protected RelativeReferences() {
        super(NessieRestApi.this);
      }
    }

    @RegisterExtension
    private final NessieClientResolverImpl proxyResolver = new NessieClientResolverImpl();

    @Override
    @Disabled
    // Disabled because NaaS proxy only proxies /trees endpoints, so /config is not available
    public void config() throws NessieNotFoundException {}

    @Override
    @Disabled
    // Disabled because NaaS proxy only proxies /trees endpoints, so /config is not available
    public void specVersion() {}

    @Disabled // Disabled because NaaS proxy only proxies /trees endpoints, so /config is not
    // available
    void invalidCreateRepositoryConfig() {}

    @Disabled // Disabled because NaaS proxy only proxies /trees endpoints, so /config is not
    // available
    void createAndUpdateRepositoryConfig() {}

    @Disabled // Disabled because NaaS proxy only proxies /trees endpoints, so /config is not
    // available
    void genericRepositoryConfigForbidden() {}
  }

  private static final class NessieClientResolverImpl extends NessieClientResolver
      implements ParameterResolver {

    private static String nessieSourcePath = String.format("v2/source/%s", DATAPLANE_PLUGIN_NAME);

    private boolean isNessieClient(ParameterContext parameterContext) {
      return parameterContext.getParameter().getType().isAssignableFrom(NessieClientFactory.class);
    }

    @Override
    public boolean supportsParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext)
        throws ParameterResolutionException {
      return isNessieClient(parameterContext) || isNessieUri(parameterContext);
    }

    @Override
    protected URI getBaseUri(ExtensionContext extensionContext) {
      return getNessieSourceUri();
    }

    private boolean isNessieUri(ParameterContext parameterContext) {
      return parameterContext.isAnnotated(NessieClientUri.class);
    }

    private URI getNessieSourceUri() {
      return getHttpClient().getNessieProxy().getUriBuilder().path(nessieSourcePath).build();
    }

    @Override
    public Object resolveParameter(
        ParameterContext parameterContext, ExtensionContext extensionContext)
        throws ParameterResolutionException {

      if (isNessieUri(parameterContext)) {
        return getNessieSourceUri();
      }

      if (isNessieClient(parameterContext)) {
        return clientFactoryForThisNessieSource();
      }

      throw new IllegalStateException("Unsupported parameter: " + parameterContext);
    }

    private NessieClientFactory clientFactoryForThisNessieSource() {
      return new NessieClientResolverImpl.ClientFactory(getNessieSourceUri());
    }

    private static final class ClientFactory implements NessieClientFactory, Serializable {

      private final URI nessieUri;

      private ClientFactory(URI nessieUri) {
        this.nessieUri = nessieUri;
      }

      @Override
      public NessieApiVersion apiVersion() {
        return NessieApiVersion.V2;
      }

      @NotNull
      @Override
      public NessieApiV2 make(NessieClientCustomizer customizer) {
        return NessieClientBuilder.createClientBuilder("HTTP", null)
            .withUri(nessieUri)
            .withApiCompatibilityCheck(
                false) // Nessie API proxy in the source does not have the /config endpoint
            .build(NessieApiV2.class);
      }
    }
  }
}
