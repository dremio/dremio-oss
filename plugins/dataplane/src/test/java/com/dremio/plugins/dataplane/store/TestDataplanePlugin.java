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

import static com.dremio.plugins.dataplane.CredentialsProviderConstants.ACCESS_KEY_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.io.file.Path;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;

/**
 * Unit tests for DataplanePlugin
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class TestDataplanePlugin {

  private static final String DATAPLANE_PLUGIN_NAME = "test_dataplane";
  @Mock
  private AbstractDataplanePluginConfig pluginConfig;
  @Mock
  private SabotContext sabotContext;
  @Mock
  private Provider<StoragePluginId> idProvider;
  @Mock
  private AWSCredentialsConfigurator awsCredentialsConfigurator;
  @Mock
  private NessieClient nessieClient;

  // Can't @InjectMocks a String, so initialization is done in @BeforeEach
  private DataplanePlugin dataplanePlugin;

  @BeforeEach
  public void setup() {
    when(awsCredentialsConfigurator.configureCredentials(any()))
      .thenReturn(ACCESS_KEY_PROVIDER);

    dataplanePlugin = new DataplanePlugin(
      pluginConfig,
      sabotContext,
      DATAPLANE_PLUGIN_NAME,
      idProvider,
      awsCredentialsConfigurator,
      nessieClient);
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
    NamespaceKey pathWithSourceName = new NamespaceKey(
      Arrays.asList(
        DATAPLANE_PLUGIN_NAME,
        folderNameWithSpace));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.createNamespace(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient).createNamespace(
      pathWithSourceName
        .getPathComponents()
        .stream().skip(1)
        .collect(Collectors.toList()),
      sourceVersion);
  }

  @Test
  public void deleteFolder() {
    final String folderName = "folder";
    final String branchName = "branchName";
    // Arrange
    NamespaceKey pathWithSourceName = new NamespaceKey(
      Arrays.asList(
        DATAPLANE_PLUGIN_NAME,
        folderName));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.deleteFolder(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient).deleteNamespace(
      pathWithSourceName
        .getPathComponents()
        .stream().skip(1)
        .collect(Collectors.toList()),
      sourceVersion);
  }

  @Test
  public void createNamespaceWithNestedFolder() {
    final String rootFolderNameWithSpace = "folder with space";
    final String leafFolderNameWithSpace = "folder with another space";
    final String branchName = "branchName";
    // Arrange
    NamespaceKey pathWithSourceName = new NamespaceKey(
      Arrays.asList(
        DATAPLANE_PLUGIN_NAME,
        rootFolderNameWithSpace,
        leafFolderNameWithSpace));
    VersionContext sourceVersion = VersionContext.ofBranch(branchName);

    // Act
    dataplanePlugin.createNamespace(pathWithSourceName, sourceVersion);

    // Assert
    verify(nessieClient).createNamespace(
      pathWithSourceName
        .getPathComponents()
        .stream().skip(1)
        .collect(Collectors.toList()),
      sourceVersion);
  }

  @Test
  public void testNessieAuthTypeSettingsCallDuringSetup() {
    when(pluginConfig.getPath())
      .thenReturn(Path.of("/test-bucket"));

    //Act
    try {
      dataplanePlugin.start();
    } catch (Exception e) {
      //ignoring this exception as this happened due to super.start() which needs extra config probably
      //This call is to verify if validateNessieAuthSettings gets called as in Assert
    }

    // Assert
    verify(pluginConfig).validateNessieAuthSettings(
      DATAPLANE_PLUGIN_NAME);
  }

  @Test
  public void testValidateConnectionToNessieCallDuringSetup() {
    when(pluginConfig.getPath())
      .thenReturn(Path.of("/test-bucket"));

    //Act
    try {
      dataplanePlugin.start();
    } catch (Exception e) {
      //ignoring this exception as this happened due to super.start() which needs extra config probably
      //This call is to verify if validateConnectionToNessieRepository gets called
    }

    // Assert
    verify(pluginConfig).validateConnectionToNessieRepository(
      nessieClient, DATAPLANE_PLUGIN_NAME, sabotContext);
  }

  @Test
  public void testInvalidURLErrorWhileValidatingNessieSpecVersion() {
    when(pluginConfig.getPath())
      .thenReturn(Path.of("/test-bucket"));
    doThrow(new InvalidURLException()).when(pluginConfig).validateNessieSpecificationVersion(nessieClient, DATAPLANE_PLUGIN_NAME);

    //Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
      .hasMessageContaining("Make sure that Nessie endpoint URL is valid");
  }

  @Test
  public void testInvalidSpecificationVersionErrorWhileValidatingNessieSpecVersion() {
    when(pluginConfig.getPath())
      .thenReturn(Path.of("/test-bucket"));
    doThrow(new InvalidSpecificationVersionException()).when(pluginConfig).validateNessieSpecificationVersion(nessieClient, DATAPLANE_PLUGIN_NAME);

    //Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
      .hasMessageContaining("Nessie Server should comply with Nessie specification version");
  }

  @Test
  public void testSemanticParserErrorWhileValidatingNessieSpecVersion() {
    when(pluginConfig.getPath())
      .thenReturn(Path.of("/test-bucket"));
    doThrow(new SemanticVersionParserException()).when(pluginConfig).validateNessieSpecificationVersion(nessieClient, DATAPLANE_PLUGIN_NAME);

    //Act + Assert
    assertThatThrownBy(() -> dataplanePlugin.start())
      .hasMessageContaining("Cannot parse Nessie specification version");
  }

  @Test
  public void testNessieApiCloseCallDuringCleanup() {
    //Act
    dataplanePlugin.close();

    // Assert
    verify(nessieClient).close();
  }

  @Test
  public void testPluginState() {
    when(pluginConfig.getState(nessieClient, DATAPLANE_PLUGIN_NAME, sabotContext)).thenReturn(SourceState.GOOD);

    //Act and Assert
    assertThat(dataplanePlugin.getState()).isEqualTo(SourceState.GOOD);

    //Assert
    verify(pluginConfig).getState(nessieClient, DATAPLANE_PLUGIN_NAME, sabotContext);
  }
}
