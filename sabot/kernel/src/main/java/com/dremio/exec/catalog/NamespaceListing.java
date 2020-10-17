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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

/**
 * Adapts listing of keys in namespace to {@link DatasetHandleListing}. This provides the listing interface by
 * performing a list of point lookups on {@link SourceMetadata} for the known datasets in the source.
 */
public class NamespaceListing implements DatasetHandleListing {

  private final NamespaceService namespaceService;
  private final NamespaceKey sourceKey;
  private final SourceMetadata sourceMetadata;
  private final DatasetRetrievalOptions options;

  public NamespaceListing(
      NamespaceService namespaceService,
      NamespaceKey sourceKey,
      SourceMetadata sourceMetadata,
      DatasetRetrievalOptions options
  ) {
    this.namespaceService = namespaceService;
    this.sourceKey = sourceKey;
    this.sourceMetadata = sourceMetadata;
    this.options = options;
  }

  @Override
  public Iterator<? extends DatasetHandle> iterator() {
    final Iterator<NamespaceKey> keyIterator;
    try {
      keyIterator = Sets.newHashSet(namespaceService.getAllDatasets(sourceKey)).iterator();
    } catch (NamespaceException e) {
      throw new RuntimeException(e);
    }

    return newIterator(keyIterator);
  }

  @VisibleForTesting
  Iterator<DatasetHandle> newIterator(Iterator<NamespaceKey> keyIterator) {
    return new TransformingIterator(keyIterator);
  }

  /**
   * Iterator that lazily transforms {@link NamespaceKey namespace keys} into {@link DatasetHandle dataset handles}.
   */
  private class TransformingIterator implements Iterator<DatasetHandle> {

    private final Iterator<NamespaceKey> keyIterator;

    private Optional<DatasetHandle> nextHandle = Optional.empty();

    TransformingIterator(Iterator<NamespaceKey> keyIterator) {
      this.keyIterator = keyIterator;
    }

    @Override
    public boolean hasNext() {
      populateNextHandle();

      return nextHandle.isPresent();
    }

    @Override
    public DatasetHandle next() {
      populateNextHandle();

      if (!nextHandle.isPresent()) {
        throw new NoSuchElementException();
      }

      final DatasetHandle handle = nextHandle.get();
      nextHandle = Optional.empty();
      return handle;
    }

    private void populateNextHandle() {
      if (nextHandle.isPresent()) {
        return;
      }

      while (keyIterator.hasNext()) {
        final NamespaceKey nextKey = keyIterator.next();

        final DatasetConfig currentConfig;
        try {
          currentConfig = namespaceService.getDataset(nextKey);
        } catch (NamespaceNotFoundException ignored) {
          continue; // race condition
        } catch (NamespaceException e) {
          throw new RuntimeException(e);
        }

        final EntityPath entityPath;
        if (currentConfig != null) {
          entityPath = new EntityPath(currentConfig.getFullPathList());
        } else {
          entityPath = MetadataObjectsUtils.toEntityPath(nextKey);
        }

        final Optional<DatasetHandle> handle;
        try {
          handle = sourceMetadata.getDatasetHandle(entityPath, options.asGetDatasetOptions(currentConfig));
        } catch (DatasetMetadataTooLargeException e) {
          throw new DatasetMetadataTooLargeException(nextKey.getSchemaPath(), e);
        } catch (ConnectorException e) {
          throw new RuntimeException(e);
        }

        if (!handle.isPresent()) {
          continue;
        }

        nextHandle = handle;
        break;
      }
    }
  }
}
