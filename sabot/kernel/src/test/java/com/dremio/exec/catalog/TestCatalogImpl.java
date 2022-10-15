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
package com.dremio.exec.catalog;

import static com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType.VALIDATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstructionWithAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.CatalogImpl.IdentityResolver;
import com.dremio.exec.catalog.CatalogServiceImpl.SourceModifier;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.store.AuthorizationContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.test.UserExceptionAssert;

public class TestCatalogImpl {

  private final MetadataRequestOptions options = mock(MetadataRequestOptions.class);
  private final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
  private final SourceModifier sourceModifier = mock(SourceModifier.class);
  private final OptionManager optionManager = mock(OptionManager.class);
  private final NamespaceService systemNamespaceService = mock(NamespaceService.class);
  private final NamespaceService.Factory namespaceFactory = mock(NamespaceService.Factory.class);
  private final Orphanage orphanage = mock(Orphanage.class);
  private final DatasetListingService datasetListingService = mock(DatasetListingService.class);
  private final ViewCreatorFactory viewCreatorFactory = mock(ViewCreatorFactory.class);
  private final SchemaConfig schemaConfig = mock(SchemaConfig.class);
  private final NamespaceService userNamespaceService = mock(NamespaceService.class);
  private final IdentityResolver identityProvider = mock(IdentityResolver.class);
  private final NamespaceIdentity namespaceIdentity = mock(NamespaceIdentity.class);
  private final VersionContextResolverImpl versionContextResolver = mock(VersionContextResolverImpl.class);
  private final String userName = "gnarly";

  @Before
  public void setup() throws Exception {
    when(options.getSchemaConfig())
      .thenReturn(schemaConfig);
    when(schemaConfig.getUserName())
      .thenReturn(userName);

    AuthorizationContext authorizationContext = new AuthorizationContext(new CatalogUser(userName), false);
    when(schemaConfig.getAuthContext()).thenReturn(authorizationContext);

    when(identityProvider.toNamespaceIdentity(authorizationContext.getSubject()))
      .thenReturn(namespaceIdentity);

    when(namespaceFactory.get(namespaceIdentity))
      .thenReturn(userNamespaceService);
  }

  private CatalogImpl newCatalogImpl(VersionContextResolverImpl versionContextResolver) {
    return new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      orphanage,
      datasetListingService,
      viewCreatorFactory,
      identityProvider,
      versionContextResolver);
  }

  @Test
  public void testUnknownSource() throws NamespaceException {
    NamespaceKey key = new NamespaceKey("unknown");
    Map<String, AttributeValue> attributes = new HashMap<>();

    CatalogImpl catalog = newCatalogImpl(null);

    doThrow(new NamespaceNotFoundException(key, "not found"))
      .when(systemNamespaceService).getDataset(key);
    when(pluginRetriever.getPlugin(key.getRoot(), true))
      .thenReturn(null);

    UserExceptionAssert.assertThatThrownBy(() -> catalog.alterDataset(key, attributes))
      .hasErrorType(VALIDATION)
      .hasMessageContaining("Unknown source");
  }

  @Test
  public void testAlterDataset_ConnectorException() throws Exception {
    ManagedStoragePlugin plugin = mock(ManagedStoragePlugin.class);
    DatasetRetrievalOptions datasetRetrievalOptions = mock(DatasetRetrievalOptions.class);
    NamespaceKey key = new NamespaceKey("hivestore.datatab");
    Map<String, AttributeValue> attributes = new HashMap<>();

    doThrow(new NamespaceNotFoundException(key, "not found"))
      .when(systemNamespaceService).getDataset(key);
    when(pluginRetriever.getPlugin(anyString(), anyBoolean()))
      .thenReturn(plugin);
    when(plugin.getDefaultRetrievalOptions())
      .thenReturn(datasetRetrievalOptions);
    doThrow(new ConnectorException())
      .when(plugin).getDatasetHandle(key, null, datasetRetrievalOptions);

    CatalogImpl catalog = newCatalogImpl(null);

    UserExceptionAssert.assertThatThrownBy(() -> catalog.alterDataset(key, attributes))
      .hasErrorType(VALIDATION)
      .hasMessageContaining("Failure while retrieving dataset");
    verify(plugin).getDatasetHandle(any(), any(), any());
  }

  @Test
  public void testAlterDataset_KeyNotFoundInDefaultNamespace() throws Exception {
    ManagedStoragePlugin plugin = mock(ManagedStoragePlugin.class);
    NamespaceKey key = new NamespaceKey("hivestore.datatab");
    Map<String, AttributeValue> attributes = new HashMap<>();
    EntityPath entityPath = mock(EntityPath.class);
    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    Optional<DatasetHandle> handle = Optional.of(datasetHandle);
    DatasetRetrievalOptions datasetRetrievalOptions = mock(DatasetRetrievalOptions.class);
    NamespaceKey namespaceKey = mock(NamespaceKey.class);

    doThrow(new NamespaceNotFoundException(key, "not found"))
      .when(systemNamespaceService).getDataset(key);
    when(pluginRetriever.getPlugin(anyString(), anyBoolean()))
      .thenReturn(plugin);
    when(plugin.getDefaultRetrievalOptions())
      .thenReturn(datasetRetrievalOptions);
    when(plugin.getDatasetHandle(key, null, datasetRetrievalOptions))
      .thenReturn(handle);
    when(datasetHandle.getDatasetPath())
      .thenReturn(entityPath);
    doThrow(new NamespaceNotFoundException(namespaceKey, "not found"))
      .when(systemNamespaceService).getDataset(namespaceKey);

    CatalogImpl catalog = newCatalogImpl(null);

    try (MockedStatic<MetadataObjectsUtils> mocked = mockStatic(MetadataObjectsUtils.class)) {
      mocked.when(() -> MetadataObjectsUtils.toNamespaceKey(entityPath)).thenReturn(namespaceKey);

      UserExceptionAssert.assertThatThrownBy(() -> catalog.alterDataset(key, attributes))
        .hasErrorType(VALIDATION)
        .hasMessageContaining("Unable to find requested dataset");
    }
    verify(plugin).getDatasetHandle(key, null, datasetRetrievalOptions);
    verify(systemNamespaceService).getDataset(namespaceKey);
  }

  @Test
  public void testAlterDataset_ShortNamespaceKey() throws Exception {
    ManagedStoragePlugin plugin = mock(ManagedStoragePlugin.class);
    NamespaceKey key = new NamespaceKey("hivestore.datatab");
    Map<String, AttributeValue> attributes = new HashMap<>();
    EntityPath entityPath = mock(EntityPath.class);
    DatasetHandle datasetHandle = mock(DatasetHandle.class);
    Optional<DatasetHandle> handle = Optional.of(datasetHandle);
    DatasetRetrievalOptions datasetRetrievalOptions = mock(DatasetRetrievalOptions.class);
    NamespaceKey namespaceKey = mock(NamespaceKey.class);
    DatasetConfig datasetConfig = new DatasetConfig();

    doThrow(new NamespaceNotFoundException(key, "not found"))
      .when(systemNamespaceService).getDataset(key);
    when(pluginRetriever.getPlugin(anyString(), anyBoolean()))
      .thenReturn(plugin);
    when(plugin.getDefaultRetrievalOptions())
      .thenReturn(datasetRetrievalOptions);
    when(plugin.getDatasetHandle(key, null, datasetRetrievalOptions))
      .thenReturn(handle);
    when(datasetHandle.getDatasetPath())
      .thenReturn(entityPath);
    when(systemNamespaceService.getDataset(namespaceKey))
      .thenReturn(datasetConfig);
    when(plugin.alterDataset(key, datasetConfig, attributes))
      .thenReturn(true);

    CatalogImpl catalog = newCatalogImpl(null);

    try (MockedStatic<MetadataObjectsUtils> mocked = mockStatic(MetadataObjectsUtils.class)) {
      mocked.when(() -> MetadataObjectsUtils.toNamespaceKey(entityPath)).thenReturn(namespaceKey);

      assertTrue(catalog.alterDataset(key, attributes));
    }
    verify(plugin).getDatasetHandle(key, null, datasetRetrievalOptions);
    verify(systemNamespaceService).getDataset(namespaceKey);
  }

  @Test
  public void testGetColumnExtendedProperties_nullReadDefinition() {
    final CatalogImpl catalog = newCatalogImpl(null);

    final DremioTable table = mock(DremioTable.class);
    when(table.getDatasetConfig()).thenReturn(DatasetConfig.getDefaultInstance());

    assertNull(catalog.getColumnExtendedProperties(table));
  }

  @Test
  public void testGetTable_ReturnsUpdatedTable() {
    final View outdatedView = mock(View.class);
    when(outdatedView.isFieldUpdated()).thenReturn(true);

    NamespaceKey viewPath = new NamespaceKey("@" + userName);

    final ViewTable tableToBeUpdated = mock(ViewTable.class);
    when(tableToBeUpdated.getView()).thenReturn(outdatedView);
    when(tableToBeUpdated.getPath()).thenReturn(viewPath);

    final ViewTable updatedTable = mock(ViewTable.class);

    ViewCreatorFactory.ViewCreator viewCreator = mock(ViewCreatorFactory.ViewCreator.class);
    when(viewCreatorFactory.get(userName)).thenReturn(viewCreator);

    NamespaceKey key = new NamespaceKey("test");

    final AtomicBoolean isFirstGetTableCall = new AtomicBoolean(true);
    try (MockedConstruction<DatasetManager> ignored = mockConstructionWithAnswer(
      DatasetManager.class,
      invocation -> {
        // generate answers for method invocations on the constructed object
        if ("getTable".equals(invocation.getMethod().getName())) {
          if (isFirstGetTableCall.getAndSet(false)) {
            return tableToBeUpdated;
          }
          return updatedTable;
        }
        // this should never get called
        return invocation.callRealMethod();
      })) {
      CatalogImpl catalog = newCatalogImpl(versionContextResolver);

      DremioTable actual = catalog.getTable(key);
      assertEquals(updatedTable, actual);
    }
  }
}
