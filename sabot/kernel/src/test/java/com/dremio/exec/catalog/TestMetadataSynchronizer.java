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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.NamespaceTestUtils;
import com.dremio.service.namespace.catalogpubsub.CatalogEventMessagePublisherProvider;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.test.DremioTest;
import java.util.Collections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMetadataSynchronizer {

  private static final String SOURCE = "test-source";
  private static final String TABLE = "test-source.public.test-table";

  private static LegacyKVStoreProvider kvStoreProvider;
  private static NamespaceService namespaceService;
  private static MetadataPolicy metadataPolicy;
  private static OptionManager optionManager;
  private static DatasetSaver datasetSaver;
  private static NamespaceKey sourceKey;
  private static SourceConfig sourceConfig;
  private static DatasetRetrievalOptions retrievalOptions;

  abstract class TestSourceMetadata implements SupportsListingDatasets, SourceMetadata {}

  @BeforeAll
  public static void setup() throws Exception {
    LocalKVStoreProvider storeProvider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    storeProvider.start();
    kvStoreProvider = storeProvider.asLegacy();
    kvStoreProvider.start();
    namespaceService =
        new NamespaceServiceImpl(
            storeProvider,
            new CatalogStatusEventsImpl(),
            CatalogEventMessagePublisherProvider.NO_OP);
    sourceKey = new NamespaceKey(SOURCE);
    sourceConfig = NamespaceTestUtils.addSource(namespaceService, SOURCE);
    metadataPolicy =
        new MetadataPolicy()
            .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
            .setDeleteUnavailableDatasets(true);
    final OptionValidatorListing optionValidatorListing =
        new OptionValidatorListingImpl(DremioTest.CLASSPATH_SCAN_RESULT);
    optionManager = new DefaultOptionManager(optionValidatorListing);
    datasetSaver = new DatasetSaverImpl(namespaceService, (NamespaceKey key) -> {}, optionManager);
    retrievalOptions =
        DatasetRetrievalOptions.DEFAULT.toBuilder().setDeleteUnavailableDatasets(true).build();
  }

  @AfterAll
  public static void teardownClass() throws Exception {
    AutoCloseables.close(kvStoreProvider);
  }

  @BeforeEach
  public void setupTest() throws Exception {
    NamespaceTestUtils.addPhysicalDS(namespaceService, TABLE);
  }

  @AfterEach
  public void cleanupTest() throws Exception {
    namespaceService.deleteSourceChildren(
        sourceKey, sourceConfig.getTag(), (DatasetConfig datasetConfig) -> {});
  }

  @Test
  public void validateSourceDatasetsNotDeletedWhenListDatasetHandlesThrows()
      throws NamespaceException, ConnectorException {
    ManagedStoragePlugin.MetadataBridge bridge = mock(ManagedStoragePlugin.MetadataBridge.class);
    SupportsListingDatasets sourceMetadata = mock(TestSourceMetadata.class);
    when(sourceMetadata.listDatasetHandles(any()))
        .thenThrow(new ConnectorException("Source error"));
    when(bridge.getMetadata()).thenReturn((SourceMetadata) sourceMetadata);
    final MetadataSynchronizer synchronizeRun =
        new MetadataSynchronizer(
            namespaceService,
            sourceKey,
            bridge,
            metadataPolicy,
            datasetSaver,
            retrievalOptions,
            optionManager);
    synchronizeRun.setup();
    synchronizeRun.go();
    Assertions.assertNotNull(
        namespaceService.getDataset(new NamespaceKey(PathUtils.parseFullPath(TABLE))));
    Assertions.assertEquals(1, namespaceService.getAllDatasetsCount(sourceKey));
  }

  @Test
  public void validateMissingDatasetsAreDeleted() throws Exception {
    ManagedStoragePlugin.MetadataBridge bridge = mock(ManagedStoragePlugin.MetadataBridge.class);
    SupportsListingDatasets sourceMetadata = mock(TestSourceMetadata.class);
    when(sourceMetadata.listDatasetHandles(any(GetDatasetOption[].class)))
        .thenReturn(Collections::emptyIterator);
    when(bridge.getMetadata()).thenReturn((SourceMetadata) sourceMetadata);
    final MetadataSynchronizer synchronizeRun =
        new MetadataSynchronizer(
            namespaceService,
            sourceKey,
            bridge,
            metadataPolicy,
            datasetSaver,
            retrievalOptions,
            optionManager);
    synchronizeRun.setup();
    synchronizeRun.go();
    Assertions.assertThrows(
        NamespaceNotFoundException.class,
        () -> namespaceService.getDataset(new NamespaceKey(PathUtils.parseFullPath(TABLE))));
    Assertions.assertEquals(0, namespaceService.getAllDatasetsCount(sourceKey));
  }
}
