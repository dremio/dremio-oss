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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.sample.SampleSourceMetadata;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

/**
 * Test namespace listing.
 */
public class TestNamespaceListing {

  private KVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;

  @Before
  public void setup() throws Exception {
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    namespaceService = new NamespaceServiceImpl(kvStoreProvider);
  }

  @After
  public void cleanup() throws Exception {
    kvStoreProvider.close();
  }

  @Test
  public void emptyIterator() {
    final NamespaceListing listing = new NamespaceListing(null, null, null, null);

    try {
      listing.newIterator(Collections.emptyIterator()).next();
      fail();
    } catch (NoSuchElementException expected) {
    }

    assertFalse(listing.newIterator(Collections.emptyIterator()).hasNext());
  }

  @Test
  public void emptyNamespace() throws NamespaceException {
    SampleSourceMetadata sourceMetadata = new SampleSourceMetadata();

    sourceMetadata.addNDatasets(10, 2, 2);
    assertTrue(sourceMetadata.listDatasetHandles().iterator().hasNext());

    final NamespaceKey sourceKey = new NamespaceKey("empty");
    namespaceService.addOrUpdateSource(sourceKey, new SourceConfig());
    assertFalse(namespaceService.getAllDatasets(sourceKey).iterator().hasNext());

    final NamespaceListing listing = new NamespaceListing(namespaceService, sourceKey, sourceMetadata,
        DatasetRetrievalOptions.DEFAULT);

    try {
      listing.newIterator(namespaceService.getAllDatasets(sourceKey).iterator()).next();
      fail();
    } catch (NoSuchElementException expected) {
    }

    assertFalse(listing.newIterator(namespaceService.getAllDatasets(sourceKey).iterator()).hasNext());
  }

  @Test
  public void typicalFlow() throws NamespaceException {
    SampleSourceMetadata sourceMetadata = new SampleSourceMetadata() {
      @Override
      public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
        final EntityPath canonicalPath = new EntityPath(datasetPath.getComponents()
            .subList(1, datasetPath.getComponents().size()));
        return super.getDatasetHandle(canonicalPath, options);
      }
    };

    sourceMetadata.addNDatasets(10, 2, 2);
    assertTrue(sourceMetadata.listDatasetHandles().iterator().hasNext());

    final NamespaceKey sourceKey = new NamespaceKey("empty");
    namespaceService.addOrUpdateSource(sourceKey, new SourceConfig());

    sourceMetadata.listDatasetHandles()
        .iterator()
        .forEachRemaining(
            datasetHandle -> {
              final List<String> canonicalComponents = ImmutableList.<String>builder()
                  .add(sourceKey.getRoot())
                  .addAll(datasetHandle.getDatasetPath().getComponents())
                  .build();
              try {
                namespaceService.addOrUpdateDataset(new NamespaceKey(canonicalComponents),
                    MetadataObjectsUtils.newShallowConfig(datasetHandle));
              } catch (NamespaceException e) {
                throw new RuntimeException(e);
              }
            }
        );
    assertTrue(namespaceService.getAllDatasets(sourceKey).iterator().hasNext());

    final NamespaceListing listing = new NamespaceListing(namespaceService, sourceKey, sourceMetadata,
        DatasetRetrievalOptions.DEFAULT);

    assertEquals(10, Iterators.size(listing.newIterator(namespaceService.getAllDatasets(sourceKey).iterator())));
  }
}
