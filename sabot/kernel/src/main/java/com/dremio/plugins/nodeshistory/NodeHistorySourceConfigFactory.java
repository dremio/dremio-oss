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
package com.dremio.plugins.nodeshistory;

import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.service.coordinator.proto.DataCredentials;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import java.net.URI;
import java.time.Duration;

/** Factory to create source config for the internal nodes history storage plugin */
public final class NodeHistorySourceConfigFactory {
  private NodeHistorySourceConfigFactory() {}

  /**
   * The metadata policy has the following characteristics: <br>
   * Metadata expire after 1 minute: This is because new files within the dataset's folder can be
   * created as often as once per minute, and they should be immediately queryable. <br>
   * Metadata never refresh: This is because the node history dataset is predicted to be write-heavy
   * and the dataset refresh frequency is unlikely to correspond well to the write frequency, so we
   * just pay the dataset refresh cost on query by ensuring it is refreshed lazily
   */
  private static final MetadataPolicy METADATA_POLICY =
      new MetadataPolicy()
          .setAuthTtlMs(CatalogService.DEFAULT_AUTHTTLS_MILLIS)
          .setDeleteUnavailableDatasets(false)
          .setAutoPromoteDatasets(false)
          .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
          .setNamesRefreshMs(CatalogService.CENTURY_MILLIS)
          .setDatasetDefinitionRefreshAfterMs(CatalogService.CENTURY_MILLIS)
          .setDatasetDefinitionExpireAfterMs(Duration.ofMinutes(1).toMillis());

  public static SourceConfig newSourceConfig(URI storagePath, DataCredentials dataCredentials) {
    return InternalFileConf.create(
        NodesHistoryStoreConfig.STORAGE_PLUGIN_NAME,
        storagePath,
        SchemaMutability.SYSTEM_TABLE,
        NodeHistorySourceConfigFactory.METADATA_POLICY,
        true,
        dataCredentials);
  }
}
