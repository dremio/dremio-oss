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
package com.dremio.plugins.dataplane.store;

import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static com.dremio.test.DremioTest.DEFAULT_DREMIO_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.Operation;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.catalog.CatalogUser;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.hadoop.security.alias.DremioCredentialProviderFactory;
import com.dremio.options.OptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.users.UserService;
import com.dremio.services.credentials.CredentialsService;
import com.google.common.base.Preconditions;

import io.findify.s3mock.S3Mock;

/**
 * Tests for DataplanePlugin
 *
 * TODO DX-54476: This was originally intended to be unit tests for
 *   DataplanePlugin, but it has expanded and become a little cluttered. These
 *   should be cleaned up and moved to TestDataplanePlugin or
 *   TestIntegrationDataplanePlugin.
 *
 * This class should be considered legacy, new tests should not be added here.
 * Instead, use one of:
 *  - TestDataplanePlugin for unit tests
 *  - TestIntegrationDataplanePlugin for integration tests
 */
@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.store.validate.namespaces", value = "false")
public class TestDataplanePlugin2 {
  // Constants
  private static final String S3_PREFIX = "s3://";
  private static final String DATAPLANE_PLUGIN_NAME = "test_dataplane";
  private static final String BUCKET_NAME = "test.dataplane.bucket";
  private static final String METADATA_FOLDER = "metadata";
  private static final String DEFAULT_BRANCH_NAME = "main";
  private static final String USER_NAME = "dataplane-test-user";
  private static final List<String> DEFAULT_TABLE_COMPONENTS =
    Arrays.asList("folderA", "folderB", "table1");
  private static final NamespaceKey DEFAULT_NAMESPACE_KEY =
    new NamespaceKey(Stream.concat(
      Stream.of(DATAPLANE_PLUGIN_NAME),
      DEFAULT_TABLE_COMPONENTS.stream())
      .collect(Collectors.toList()));
  private static final VersionContext DEFAULT_VERSION_CONTEXT =
    VersionContext.ofBranch(DEFAULT_BRANCH_NAME);
  private static final BatchSchema DEFAULT_BATCH_SCHEMA =
    BatchSchema.newBuilder().build();

  private static S3Mock s3Mock;
  private static int S3_PORT;
  @TempDir
  static File temporaryDirectory;
  private static Path bucketPath;

  // Nessie
  private static String nessieUri;
  @NessieBaseUri
  private static URI nessieBaseUri;
  @NessieAPI
  private static NessieApiV1 nessieClient;

  // Dataplane Plugin
  private static DataplanePlugin dataplanePlugin;
  private static DataplanePlugin dataplanePluginNotAuthorized;
  private static DataplanePlugin dataplanePluginInvalidAWSBucket;

  @BeforeAll
  public static void setUp() throws Exception {
    DremioCredentialProviderFactory.configure(() ->
      CredentialsService.newInstance(DEFAULT_DREMIO_CONFIG, CLASSPATH_SCAN_RESULT));
    setUpS3Mock();
    setUpNessie();
    setUpDataplanePlugin();
    setUpDataplanePluginNotAuthorized();
  }

  private static void setUpS3Mock() throws IOException {
    bucketPath = Paths.get(temporaryDirectory.getAbsolutePath(), BUCKET_NAME);
    Files.createDirectory(bucketPath);

    Preconditions.checkState(s3Mock == null);
    s3Mock = new S3Mock.Builder()
      .withPort(0)
      .withFileBackend(temporaryDirectory.getAbsolutePath())
      .build();
    S3_PORT = s3Mock.start().localAddress().getPort();
  }

  private static void setUpNessie() {
    nessieUri = nessieBaseUri.resolve("v2").toString();
  }

  private static void setUpDataplanePluginNotAuthorized() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieUri;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = "bar"; // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = BUCKET_NAME;

    SabotContext context = mock(SabotContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    UserService userService = mock(UserService.class);
    when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(true);
    when(context.getOptionManager()).thenReturn(optionManager);
    when(context.getUserService()).thenReturn(userService);

    // S3Mock settings
    nessiePluginConfig.propertyList = Arrays.asList(
      new Property("fs.s3a.endpoint", "localhost:" + S3_PORT),
      new Property("fs.s3a.path.style.access", "true"),
      new Property(S3FileSystem.COMPATIBILITY_MODE, "true")
    );

    NessiePluginConfig mockNessiePluginConfig = spy(nessiePluginConfig);
    NessieClient nessieClient = mock(NessieClient.class);
    NessieApiV2 nessieApi = mock(NessieApiV2.class);
    NessieConfiguration nessieConfiguration = mock(NessieConfiguration.class);
    when(nessieClient.getNessieApi()).thenReturn(nessieApi);
    when(nessieApi.getConfig()).thenReturn(nessieConfiguration);
    when(nessieConfiguration.getSpecVersion()).thenReturn(MINIMUM_NESSIE_SPECIFICATION_VERSION);
    when(nessieClient.getDefaultBranch()).thenThrow(UnAuthenticatedException.class);
    when(mockNessiePluginConfig.getNessieClient(DATAPLANE_PLUGIN_NAME, context)).thenReturn(nessieClient);

    dataplanePluginNotAuthorized = spy(mockNessiePluginConfig.newPlugin(
      context,
      DATAPLANE_PLUGIN_NAME,
      null));
  }

  private static void setUpDataplanePluginInvalidAWSBucket() {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieUri;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = "bar"; // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = "/";

    SabotContext context = mock(SabotContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    UserService userService = mock(UserService.class);
    when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(true);
    when(context.getOptionManager()).thenReturn(optionManager);
    when(context.getUserService()).thenReturn(userService);

    // S3Mock settings
    nessiePluginConfig.propertyList = Arrays.asList(
      new Property("fs.s3a.endpoint", "localhost:" + S3_PORT),
      new Property("fs.s3a.path.style.access", "true"),
      new Property(S3FileSystem.COMPATIBILITY_MODE, "true")
    );

    dataplanePluginInvalidAWSBucket = spy(nessiePluginConfig.newPlugin(
      context,
      DATAPLANE_PLUGIN_NAME,
      null));
  }

  private static void setUpDataplanePlugin() throws Exception {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = nessieUri;
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = "bar"; // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = BUCKET_NAME;

    SabotContext context = mock(SabotContext.class);
    OptionManager optionManager = mock(OptionManager.class);
    UserService userService = mock(UserService.class);
    when(optionManager.getOption(NESSIE_PLUGIN_ENABLED)).thenReturn(true);
    when(context.getUserService()).thenReturn(userService);
    when(context.getOptionManager()).thenReturn(optionManager);
    when(context.getClasspathScan()).thenReturn(CLASSPATH_SCAN_RESULT);
    when(context.getFileSystemWrapper()).thenReturn((fs, storageId, conf, operatorContext, enableAsync, isMetadataEnabled) -> fs);

    // S3Mock settings
    nessiePluginConfig.propertyList = Arrays.asList(
      new Property("fs.s3a.endpoint", "localhost:" + S3_PORT),
      new Property("fs.s3a.path.style.access", "true"),
      new Property(S3FileSystem.COMPATIBILITY_MODE, "true")
    );

    dataplanePlugin = nessiePluginConfig.newPlugin(
      context,
      DATAPLANE_PLUGIN_NAME,
      null);
    dataplanePlugin.start();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    AutoCloseables.close(dataplanePlugin, dataplanePluginNotAuthorized, dataplanePluginInvalidAWSBucket, nessieClient);
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  @Test
  public void testInvalidAWSRootPathErrorDuringSetup() {
    assertThatThrownBy(TestDataplanePlugin2::setUpDataplanePluginInvalidAWSBucket)
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Failure creating or updating Nessie source. Invalid AWS Root Path.");
  }

  @Test
  public void testNessieWrongToken() throws Exception {
    // act+assert
    assertThatThrownBy(()->dataplanePluginNotAuthorized.start())
      .isInstanceOf(UserException.class)
      .hasMessageContaining("Unable to authenticate to the Nessie server");
  }

  @Test
  public void createEmptyTable()
      throws NessieNotFoundException, ReferenceNotFoundException,
        NoDefaultBranchException, ReferenceConflictException, IOException {
    // Arrange

    // Act
    dataplanePlugin.createEmptyTable(
      DEFAULT_NAMESPACE_KEY,
      getSchemaConfig(),
      DEFAULT_BATCH_SCHEMA,
      makeWriterOptions());

    // Assert
    assertNessieHasCommitForTable(DEFAULT_TABLE_COMPONENTS, Operation.Put.class);
    assertNessieHasTable(DEFAULT_TABLE_COMPONENTS);
    assertIcebergTableExistsAtSubPath(DEFAULT_TABLE_COMPONENTS);
  }

  @Test
  public void createEmptyTableBadPluginName() {
    // Arrange
    NamespaceKey tableKeyWithPluginName = new NamespaceKey(
      Stream.concat(
          Stream.of("bad" + DATAPLANE_PLUGIN_NAME),
          DEFAULT_TABLE_COMPONENTS.stream())
        .collect(Collectors.toList()));

    // Act + Assert
    assertThatThrownBy(() ->
      dataplanePlugin.createEmptyTable(
        tableKeyWithPluginName,
        null,
        DEFAULT_BATCH_SCHEMA,
        makeWriterOptions()))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createEmptyTableOnlyPluginName() {
    // Arrange
    NamespaceKey justPluginName = new NamespaceKey(DATAPLANE_PLUGIN_NAME);

    // Act + Assert
    assertThatThrownBy(() ->
      dataplanePlugin.createEmptyTable(
        justPluginName,
        getSchemaConfig(),
        DEFAULT_BATCH_SCHEMA,
        makeWriterOptions()))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createEmptyTableNoVersionContext() {
    // Arrange
    WriterOptions noVersionContext = WriterOptions.DEFAULT
      .withPartitionColumns(null);

    // Act + Assert
    assertThatThrownBy(() ->
      dataplanePlugin.createEmptyTable(
        DEFAULT_NAMESPACE_KEY,
        getSchemaConfig(),
        DEFAULT_BATCH_SCHEMA,
        noVersionContext))
      .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void dropTable() throws Exception {

    // Arrange
    dataplanePlugin.createEmptyTable(
      DEFAULT_NAMESPACE_KEY,
      getSchemaConfig(),
      DEFAULT_BATCH_SCHEMA,
      makeWriterOptions());

    // Act
    dataplanePlugin.dropTable(DEFAULT_NAMESPACE_KEY,
      getSchemaConfig(),
    defaultTableOption());

    // Assert
    assertNessieHasCommitForTable(DEFAULT_TABLE_COMPONENTS, Operation.Delete.class);
    assertNessieDoesNotHaveTable(DEFAULT_TABLE_COMPONENTS);

    // TODO For now, we aren't doing filesystem cleanup, so this check is correct. Might change in the future.
    assertIcebergTableExistsAtSubPath(DEFAULT_TABLE_COMPONENTS);
  }

  private WriterOptions makeWriterOptions()
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    return WriterOptions.DEFAULT
      .withPartitionColumns(null)
      .withVersion(dataplanePlugin.resolveVersionContext(DEFAULT_VERSION_CONTEXT));
  }

  private SchemaConfig getSchemaConfig() {
    return SchemaConfig.newBuilder(new CatalogUser(USER_NAME)).build();
  }

  private TableMutationOptions defaultTableOption()
    throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    ResolvedVersionContext resolvedVersionContext = dataplanePlugin.resolveVersionContext(DEFAULT_VERSION_CONTEXT);
    return TableMutationOptions.newBuilder()
      .setResolvedVersionContext(resolvedVersionContext)
      .build();
  }

  private void assertNessieHasCommitForTable(
    List<String> tableSchemaComponents,
    Class<? extends Operation> operationType)
      throws NessieNotFoundException {
    final List<LogEntry> logEntries = nessieClient
      .getCommitLog()
      .refName(DEFAULT_BRANCH_NAME)
      .fetch(FetchOption.ALL) // Get extended data, including operations
      .get()
      .getLogEntries();
    assertTrue(logEntries.size() >= 1);
    final LogEntry mostRecentLogEntry = logEntries.get(0); // Commits are ordered most recent to earliest

    assertThat(mostRecentLogEntry.getCommitMeta().getAuthor()).isEqualTo(USER_NAME);

    final List<Operation> operations = mostRecentLogEntry.getOperations();
    assertEquals(1, operations.size());
    final Operation operation = operations.get(0);
    assertTrue(operationType.isAssignableFrom(operation.getClass()));

    final ContentKey actualContentKey = operation.getKey();
    final ContentKey expectedContentKey = ContentKey.of(tableSchemaComponents);
    assertEquals(expectedContentKey, actualContentKey);
  }

  private void assertNessieHasTable(List<String> tableSchemaComponents)
      throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap = nessieClient
      .getContent()
      .refName(DEFAULT_BRANCH_NAME)
      .key(ContentKey.of(tableSchemaComponents))
      .get();

    ContentKey expectedContentsKey = ContentKey.of(tableSchemaComponents);
    assertTrue(contentsMap.containsKey(expectedContentsKey));

    String expectedMetadataLocationPrefix = S3_PREFIX + BUCKET_NAME + "/" +
      String.join("/", tableSchemaComponents) + "/" + METADATA_FOLDER;
    Optional<IcebergTable> maybeIcebergTable = contentsMap
      .get(expectedContentsKey)
      .unwrap(IcebergTable.class);
    assertTrue(maybeIcebergTable.isPresent());
    assertTrue(maybeIcebergTable.get()
      .getMetadataLocation()
      .startsWith(expectedMetadataLocationPrefix));
  }

  private void assertNessieDoesNotHaveTable(List<String> tableSchemaComponents)
      throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap = nessieClient
      .getContent()
      .refName(DEFAULT_BRANCH_NAME)
      .key(ContentKey.of(tableSchemaComponents))
      .get();

    assertTrue(contentsMap.isEmpty());
  }

  private void assertIcebergTableExistsAtSubPath(List<String> subPath) {
    // Iceberg tables on disk have a "metadata" folder in their root, check for "metadata" folder too
    Path pathToMetadataFolder = bucketPath
      .resolve(String.join("/", subPath))
      .resolve(METADATA_FOLDER);

    assertTrue(Files.exists(pathToMetadataFolder));
  }
}
