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

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.CatalogServiceImpl.SourceModifier;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.options.OptionManager;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.test.UserExceptionMatcher;

@RunWith(PowerMockRunner.class)
@PrepareForTest(MetadataObjectsUtils.class)
public class TestCatalogImpl {

  private final MetadataRequestOptions options = mock(MetadataRequestOptions.class);
  private final PluginRetriever pluginRetriever = mock(PluginRetriever.class);
  private final SourceModifier sourceModifier = mock(SourceModifier .class);
  private final OptionManager optionManager = mock(OptionManager.class);
  private final NamespaceService systemNamespaceService = mock(NamespaceService.class);
  private final NamespaceService.Factory namespaceFactory = mock(NamespaceService.Factory.class);
  private final DatasetListingService datasetListingService = mock(DatasetListingService.class);
  private final ViewCreatorFactory viewCreatorFactory = mock(ViewCreatorFactory.class);
  private final SchemaConfig schemaConfig = mock(SchemaConfig.class);
  private final InformationSchemaCatalogImpl iscDelegate = mock(InformationSchemaCatalogImpl.class);
  private final DatasetManager datasets = mock(DatasetManager.class);
  private final NamespaceService userNamespaceService = mock(NamespaceService.class);


  @Rule
  public final ExpectedException thrownException = ExpectedException.none();

  @Before
  public void setup() throws Exception {
    final String userName = "gnarly";

    when(options.getSchemaConfig())
      .thenReturn(schemaConfig);
    when(schemaConfig.getUserName())
      .thenReturn(userName);
    when(namespaceFactory.get(userName))
      .thenReturn(userNamespaceService);
    whenNew(DatasetManager.class)
      .withArguments(pluginRetriever, userNamespaceService, optionManager, userName)
      .thenReturn(datasets);
    whenNew(InformationSchemaCatalogImpl.class)
      .withArguments(userNamespaceService)
      .thenReturn(iscDelegate);
  }

  @Test
  public void testUnknownSource() throws NamespaceException {
    NamespaceKey key = new NamespaceKey("unknown");
    Map<String, AttributeValue> attributes = new HashMap<>();

    CatalogImpl catalog = new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);

    doThrow(new NamespaceNotFoundException(key, "not found"))
      .when(systemNamespaceService).getDataset(key);
    when(pluginRetriever.getPlugin(key.getRoot(), true))
      .thenReturn(null);

    thrownException.expect(
      new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
        "Unknown source"));
    catalog.alterDataset(key, attributes);
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

    CatalogImpl catalog = new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);

    thrownException.expect(
      new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
        "Failure while retrieving dataset"));

    catalog.alterDataset(key, attributes);
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
    PowerMockito.mockStatic(MetadataObjectsUtils.class);

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
    when(MetadataObjectsUtils.toNamespaceKey(entityPath))
      .thenReturn(namespaceKey);
    doThrow(new NamespaceNotFoundException(namespaceKey, "not found"))
      .when(systemNamespaceService).getDataset(namespaceKey);

    CatalogImpl catalog = new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);

    thrownException.expect(
      new UserExceptionMatcher(UserBitShared.DremioPBError.ErrorType.VALIDATION,
        "Unable to find requested dataset"));
    catalog.alterDataset(key, attributes);
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
    PowerMockito.mockStatic(MetadataObjectsUtils.class);
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
    when(MetadataObjectsUtils.toNamespaceKey(entityPath))
      .thenReturn(namespaceKey);
    when(systemNamespaceService.getDataset(namespaceKey))
      .thenReturn(datasetConfig);
    when(plugin.alterDataset(key, datasetConfig, attributes))
      .thenReturn(true);

    CatalogImpl catalog = new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);

    assertTrue(catalog.alterDataset(key, attributes));
    verify(plugin).getDatasetHandle(key, null, datasetRetrievalOptions);
    verify(systemNamespaceService).getDataset(namespaceKey);
  }

  @Test
  public void testGetColumnExtendedProperties_nullReadDefinition() {
    final CatalogImpl catalog = new CatalogImpl(options,
      pluginRetriever,
      sourceModifier,
      optionManager,
      systemNamespaceService,
      namespaceFactory,
      datasetListingService,
      viewCreatorFactory);

    final DremioTable table = mock(DremioTable.class);
    when(table.getDatasetConfig()).thenReturn(DatasetConfig.getDefaultInstance());

    assertNull(catalog.getColumnExtendedProperties(table));
  }
}
