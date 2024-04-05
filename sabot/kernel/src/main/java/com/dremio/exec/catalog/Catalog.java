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

import com.dremio.exec.catalog.namespace.NamespacePassthrough;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.store.PartitionNotFoundException;
import com.dremio.service.namespace.NamespaceAttribute;
import com.dremio.service.namespace.NamespaceKey;
import java.io.IOException;
import java.util.List;

/**
 * Interface used to retrieve virtual and physical datasets. This is always contextualized to a
 * single user and default schema. Implementations must be thread-safe
 */
public interface Catalog
    extends SimpleCatalog<Catalog>,
        EntityExplorer,
        DatasetCatalog,
        SourceCatalog,
        InformationSchemaCatalog,
        VersionContextResolver,
        NamespacePassthrough {
  /**
   * Resolve an ambiguous reference using the following rules: if the reference is a single value
   * and a default schema is defined, resolve using the default schema. Otherwise, resolve using the
   * name directly.
   *
   * @param key
   * @return
   */
  NamespaceKey resolveSingle(NamespaceKey key);

  /**
   * Resolve the provided key to the default schema path, if there is one.
   *
   * @param key
   * @return
   */
  NamespaceKey resolveToDefault(NamespaceKey key);

  /**
   * Return a new Catalog contextualized to the provided subject
   *
   * @param subject
   * @return
   */
  Catalog resolveCatalog(CatalogIdentity subject);

  default MetadataStatsCollector getMetadataStatsCollector() {
    return getMetadataRequestOptions().getStatsCollector();
  }

  // TODO(DX-21034): Rework View Creator
  void createView(
      final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException;

  // TODO(DX-21034): Rework View Creator
  void updateView(
      final NamespaceKey key, View view, ViewOptions viewOptions, NamespaceAttribute... attributes)
      throws IOException;

  // TODO(DX-21034): Rework View Creator
  void dropView(final NamespaceKey key, ViewOptions viewOptions) throws IOException;

  Iterable<String> getSubPartitions(
      NamespaceKey key, List<String> partitionColumns, List<String> partitionValues)
      throws PartitionNotFoundException;

  default void addCatalogStats() {}

  default void invalidateNamespaceCache(final NamespaceKey key) {}

  MetadataRequestOptions getMetadataRequestOptions();
}
