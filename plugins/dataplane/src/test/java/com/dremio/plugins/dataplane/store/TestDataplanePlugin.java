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

import static com.dremio.catalog.model.VersionContext.NOT_SPECIFIED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AWS_STORAGE_ENABLED;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.BYPASS_DATAPLANE_CACHE;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES;
import static com.dremio.nessiemetadata.cache.NessieMetadataCacheOptions.DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.ImmutableVersionedListOptions;
import com.dremio.exec.catalog.ImmutableVersionedListResponsePage;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VersionedListOptions;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.InvalidNessieApiVersionException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.nessiemetadata.cache.NessieDataplaneCacheProvider;
import com.dremio.nessiemetadata.cache.NessieDataplaneCaffeineCacheProvider;
import com.dremio.nessiemetadata.storeprovider.NessieDataplaneCacheStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.ImmutableNessieListOptions;
import com.dremio.plugins.ImmutableNessieListResponsePage;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.dataplane.store.AbstractDataplanePluginConfig.StorageProviderType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieForbiddenException;

/** Unit tests for DataplanePlugin */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TestDataplanePlugin {

  private static final String DATAPLANE_PLUGIN_NAME = "test_dataplane";
  private final NessieDataplaneCacheProvider cacheProvider =
      new NessieDataplaneCaffeineCacheProvider();
  @Mock private AbstractDataplanePluginConfig pluginConfig;
  @Mock private SabotContext sabotContext;
  @Mock private OptionManager optionManager;
  @Mock private Provider<StoragePluginId> idProvider;
  @Mock private static NessieClient nessieClient;

  // Can't @InjectMocks a String, so initialization is done in @BeforeEach
  private DataplanePlugin dataplanePlugin;

  private static class DataplanePluginMockImpl extends DataplanePlugin {

    public DataplanePluginMockImpl(
        AbstractDataplanePluginConfig pluginConfig,
        SabotContext context,
        String name,
        Provider<StoragePluginId> idProvider,
        NessieDataplaneCacheProvider cacheProvider,
        @Nullable NessieDataplaneCacheStoreProvider nessieDataplaneCacheStoreProvider) {
      super(
          pluginConfig,
          context,
          name,
          idProvider,
          cacheProvider,
          nessieDataplaneCacheStoreProvider);
    }

    @Override
    public SourceState getState(NessieClient nessieClient, String name, SabotContext context) {
      return SourceState.GOOD;
    }

    @Override
    public void validatePluginEnabled(SabotContext context) {
      // no-op
    }

    @Override
    public void validateConnectionToNessieRepository(
        NessieClient nessieClient, String name, SabotContext context) {
      // no-op
    }

    @Override
    public void validateNessieSpecificationVersion(NessieClient nessieClient) {
      // no-op
    }

    @Override
    public NessieClient getNessieClient() {
      return nessieClient;
    }
  }

  @BeforeEach
  public void setup() {
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_EXPIRE_AFTER_ACCESS_MINUTES);
    doReturn(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS.getDefault().getNumVal())
        .when(optionManager)
        .getOption(DATAPLANE_ICEBERG_METADATA_CACHE_SIZE_ITEMS);
    doReturn(BYPASS_DATAPLANE_CACHE.getDefault().getBoolVal())
        .when(optionManager)
        .getOption(BYPASS_DATAPLANE_CACHE);

    dataplanePlugin =
        new DataplanePluginMockImpl(
            pluginConfig, sabotContext, DATAPLANE_PLUGIN_NAME, idProvider, cacheProvider, null);
  }

  @Test
  public void createTag() {
    // Arrange
    String tagName = "tagName";
    VersionContext sourceVersion = VersionContext.ofBranch("branchName");

    // Act
    dataplanePlugin.createTag(tagName, sourceVersion);

    // Assert
    verify(nessieClient).createTag(tagName, sourceVersion);
  }

  @Test
  public void createNamespace() {
    final String folderNameWithSpace = "folder with space";
    final String branchName = "branchName";
    // Arrange
    NamespaceKey pathWithSourceName =
        new NamespaceKey(Arrays.asList(DATAPLANE_PLUGIN_NAME, folderNameWithSpace));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.createNamespace(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient)
        .createNamespace(
            pathWithSourceName.getPathComponents().stream().skip(1).collect(Collectors.toList()),
            sourceVersion);
  }

  @Test
  public void deleteFolder() {
    final String folderName = "folder";
    final String branchName = "branchName";
    // Arrange
    NamespaceKey pathWithSourceName =
        new NamespaceKey(Arrays.asList(DATAPLANE_PLUGIN_NAME, folderName));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.deleteFolder(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient)
        .deleteNamespace(
            pathWithSourceName.getPathComponents().stream().skip(1).collect(Collectors.toList()),
            sourceVersion);
  }

  @Test
  public void testValidateCallsDuringSetup() {
    setupAWSStorageAndProvider();
    dataplanePlugin = spy(dataplanePlugin);
    // Act
    try {
      dataplanePlugin.start();
    } catch (Exception e) {
      // ignoring this exception as this happened due to super.start() which needs extra config
      // probably
      // This call is to verify if start invokes all validation calls appropriately
    }

    // Assert
    verify(dataplanePlugin).validateStorageProviderTypeEnabled(optionManager);
    verify(dataplanePlugin).validatePluginEnabled(sabotContext);
    verify(dataplanePlugin).validateRootPath();
    verify(dataplanePlugin)
        .validateConnectionToNessieRepository(nessieClient, DATAPLANE_PLUGIN_NAME, sabotContext);
    verify(dataplanePlugin).validateNessieSpecificationVersion(nessieClient);
  }

  @Test
  public void testInvalidURLErrorWhileValidatingNessieSpecVersion() {
    setupAWSStorageAndProvider();
    dataplanePlugin = spy(dataplanePlugin);
    doThrow(new InvalidURLException("Make sure that Nessie endpoint URL is valid"))
        .when(dataplanePlugin)
        .validateNessieSpecificationVersion(nessieClient);

    // Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Make sure that Nessie endpoint URL is valid");
  }

  @Test
  public void testIncompatibleNessieApiInEndpointURL() {
    setupAWSStorageAndProvider();
    dataplanePlugin = spy(dataplanePlugin);
    doThrow(new InvalidNessieApiVersionException("Invalid API version."))
        .when(dataplanePlugin)
        .validateNessieSpecificationVersion(nessieClient);

    // Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Invalid API version.");
  }

  @Test
  public void testInvalidSpecificationVersionErrorWhileValidatingNessieSpecVersion() {
    setupAWSStorageAndProvider();
    dataplanePlugin = spy(dataplanePlugin);
    doThrow(
            new InvalidSpecificationVersionException(
                "Nessie Server should comply with Nessie specification version"))
        .when(dataplanePlugin)
        .validateNessieSpecificationVersion(nessieClient);

    // Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Nessie Server should comply with Nessie specification version");
  }

  @Test
  public void testSemanticParserErrorWhileValidatingNessieSpecVersion() {
    setupAWSStorageAndProvider();
    dataplanePlugin = spy(dataplanePlugin);
    doThrow(new SemanticVersionParserException("Cannot parse Nessie specification version"))
        .when(dataplanePlugin)
        .validateNessieSpecificationVersion(nessieClient);

    // Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
        .isInstanceOf(UserException.class)
        .hasMessageContaining("Cannot parse Nessie specification version");
  }

  @Test
  public void createNamespaceWithNestedFolder() {
    final String rootFolderNameWithSpace = "folder with space";
    final String leafFolderNameWithSpace = "folder with another space";
    final String branchName = "branchName";
    // Arrange
    NamespaceKey pathWithSourceName =
        new NamespaceKey(
            Arrays.asList(DATAPLANE_PLUGIN_NAME, rootFolderNameWithSpace, leafFolderNameWithSpace));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.createNamespace(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient)
        .createNamespace(
            pathWithSourceName.getPathComponents().stream().skip(1).collect(Collectors.toList()),
            sourceVersion);
  }

  @Test
  public void testNessieApiCloseCallDuringCleanup() {
    // Act
    dataplanePlugin.close();

    // Assert
    verify(nessieClient).close();
  }

  @Test
  public void testValidatePath() {
    when(pluginConfig.getRootPath()).thenReturn("");

    assertThat(dataplanePlugin.resolveTableNameToValidPath(Collections.emptyList())).isEmpty();

    final String sourceNameWithoutDot = "source";
    final String folderName = "folder";
    final String tableName = "table";

    List<String> tablePath = Arrays.asList(sourceNameWithoutDot, folderName, tableName);

    when(pluginConfig.getRootPath()).thenReturn(sourceNameWithoutDot);

    assertThat(dataplanePlugin.resolveTableNameToValidPath(tablePath)).isEqualTo(tablePath);

    final String sourceNameWithDot = "source.1";
    tablePath = Arrays.asList(sourceNameWithDot, folderName, tableName);

    when(pluginConfig.getRootPath()).thenReturn(sourceNameWithDot);

    assertThat(dataplanePlugin.resolveTableNameToValidPath(tablePath)).isEqualTo(tablePath);

    final String sourceNameWithSlash = "source/folder1/folder2";
    List<String> fullPath =
        Streams.concat(
                Arrays.stream(sourceNameWithSlash.split("/")), Stream.of(folderName, tableName))
            .collect(Collectors.toList());
    tablePath = Arrays.asList(sourceNameWithSlash, folderName, tableName);

    when(pluginConfig.getRootPath()).thenReturn(sourceNameWithSlash);

    assertThat(dataplanePlugin.resolveTableNameToValidPath(tablePath)).isEqualTo(fullPath);

    final String sourceNameWithSlashAndDot = "source.1/folder1.1/folder2.2";
    fullPath =
        Streams.concat(
                Arrays.stream(sourceNameWithSlashAndDot.split("/")),
                Stream.of(folderName, tableName))
            .collect(Collectors.toList());
    tablePath = Arrays.asList(sourceNameWithSlashAndDot, folderName, tableName);

    when(pluginConfig.getRootPath()).thenReturn(sourceNameWithSlashAndDot);

    assertThat(dataplanePlugin.resolveTableNameToValidPath(tablePath)).isEqualTo(fullPath);
  }

  @Test
  public void testHandlesNessieForbiddenException() {
    VersionedDatasetAccessOptions versionedDatasetAccessOptions =
        mock(VersionedDatasetAccessOptions.class);
    when(versionedDatasetAccessOptions.getVersionContext())
        .thenReturn(mock(ResolvedVersionContext.class));
    doThrow(
            new NessieForbiddenException(
                ImmutableNessieError.builder()
                    .errorCode(ErrorCode.FORBIDDEN)
                    .reason("A reason")
                    .message("A message")
                    .status(403)
                    .build()))
        .when(nessieClient)
        .getContent(any(), any(), any());

    assertThatThrownBy(
            () ->
                dataplanePlugin.getDatasetHandle(
                    new EntityPath(Arrays.asList("Nessie", "mytable")),
                    versionedDatasetAccessOptions))
        .isInstanceOf(AccessControlException.class)
        .hasMessageContaining("403");
  }

  @Test
  public void testGetAllTableInfoErrorReturnsEmptyStream() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(nessieClient.getDefaultBranch()).thenReturn(resolvedVersionContext);
    when(nessieClient.listEntries(
            null,
            resolvedVersionContext,
            NessieClient.NestingMode.INCLUDE_NESTED_CHILDREN,
            NessieClient.ContentMode.ENTRY_WITH_CONTENT,
            EnumSet.of(ExternalNamespaceEntry.Type.ICEBERG_TABLE),
            null))
        .thenThrow(new ReferenceNotFoundException("foo"));
    assertThat(dataplanePlugin.getAllTableInfo()).isEmpty();
  }

  @Test
  public void testGetAllTableInfoEmptyStreamReturnsEmptyStream() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(nessieClient.getDefaultBranch()).thenReturn(resolvedVersionContext);
    when(nessieClient.listEntries(
            null,
            resolvedVersionContext,
            NessieClient.NestingMode.INCLUDE_NESTED_CHILDREN,
            NessieClient.ContentMode.ENTRY_WITH_CONTENT,
            EnumSet.of(ExternalNamespaceEntry.Type.ICEBERG_TABLE),
            null))
        .thenReturn(Stream.empty());
    assertThat(dataplanePlugin.getAllTableInfo()).isEmpty();
  }

  @Test
  public void testGetAllViewInfoEmptyStreamReturnsEmptyStream() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(nessieClient.getDefaultBranch()).thenReturn(resolvedVersionContext);
    when(nessieClient.listEntries(
            null,
            resolvedVersionContext,
            NessieClient.NestingMode.INCLUDE_NESTED_CHILDREN,
            NessieClient.ContentMode.ENTRY_WITH_CONTENT,
            EnumSet.of(ExternalNamespaceEntry.Type.ICEBERG_VIEW),
            null))
        .thenReturn(Stream.empty());
    assertThat(dataplanePlugin.getAllViewInfo()).isEmpty();
  }

  @Test
  public void testGetAllViewInfoErrorReturnsEmptyStream() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    when(nessieClient.getDefaultBranch()).thenReturn(resolvedVersionContext);
    when(nessieClient.listEntries(
            null,
            resolvedVersionContext,
            NessieClient.NestingMode.INCLUDE_NESTED_CHILDREN,
            NessieClient.ContentMode.ENTRY_WITH_CONTENT,
            EnumSet.of(ExternalNamespaceEntry.Type.ICEBERG_VIEW),
            null))
        .thenReturn(Stream.empty());
    assertThat(dataplanePlugin.getAllViewInfo()).isEmpty();
  }

  @Test
  public void testListEntries_convert() {
    ResolvedVersionContext resolvedVersionContext = mock(ResolvedVersionContext.class);
    VersionContext versionContext = mock(VersionContext.class);
    when(nessieClient.resolveVersionContext(eq(versionContext))).thenReturn(resolvedVersionContext);

    ExternalNamespaceEntry entry =
        ExternalNamespaceEntry.of(
            ExternalNamespaceEntry.Type.FOLDER, ImmutableList.of("a", "b"), null, null, null);
    String previousPageToken = "previous-token";
    int maxResultsPerPage = 10;
    String pageToken = "token";
    when(nessieClient.listEntriesPage(
            null,
            resolvedVersionContext,
            NessieClient.NestingMode.IMMEDIATE_CHILDREN_ONLY,
            NessieClient.ContentMode.ENTRY_METADATA_ONLY,
            null,
            null,
            new ImmutableNessieListOptions.Builder()
                .setPageToken(previousPageToken)
                .setMaxResultsPerPage(maxResultsPerPage)
                .build()))
        .thenReturn(
            new ImmutableNessieListResponsePage.Builder()
                .addEntries(entry)
                .setPageToken(pageToken)
                .build());

    // Verify response conversion.
    VersionedListOptions options =
        new ImmutableVersionedListOptions.Builder()
            .setPageToken(previousPageToken)
            .setMaxResultsPerPage(maxResultsPerPage)
            .build();
    assertThat(dataplanePlugin.listEntriesPage(null, versionContext, options))
        .isEqualTo(
            new ImmutableVersionedListResponsePage.Builder()
                .addEntries(entry)
                .setPageToken(pageToken)
                .build());
  }

  @Test
  public void testGetFunctionsNessieForbiddenException() {
    doThrow(
            new NessieForbiddenException(
                ImmutableNessieError.builder()
                    .errorCode(ErrorCode.FORBIDDEN)
                    .reason("No one shall pass!")
                    .message("You shall not pass!")
                    .status(403)
                    .build()))
        .when(nessieClient)
        .resolveVersionContext(any());

    assertThat(dataplanePlugin.getFunctions(NOT_SPECIFIED)).isEmpty();
  }

  private void setupAWSStorageAndProvider() {
    when(optionManager.getOption(DATAPLANE_AWS_STORAGE_ENABLED)).thenReturn(true);
    when(sabotContext.getOptionManager()).thenReturn(optionManager);
    when(pluginConfig.getStorageProvider()).thenReturn(StorageProviderType.AWS);
    when(pluginConfig.getRootPath()).thenReturn("/bucket/folder");
  }
}
