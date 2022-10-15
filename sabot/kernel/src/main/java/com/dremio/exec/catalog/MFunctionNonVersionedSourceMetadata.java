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

import static com.dremio.exec.catalog.CatalogUtil.getTimeTravelRequest;

import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

/**
 * Table functions metadata for plugin except data plane.
 */
public class MFunctionNonVersionedSourceMetadata extends MFunctionMetadataImpl {

  private final DatasetConfig underlyingTableConfig;

  public MFunctionNonVersionedSourceMetadata(NamespaceKey canonicalKey, DatasetConfig currentConfig, ManagedStoragePlugin plugin,
                                             SchemaConfig schemaConfig, TableVersionContext context) {
    super(canonicalKey, currentConfig, plugin, schemaConfig, context);
    this.underlyingTableConfig = currentConfig;
  }

  @Override
  public DatasetRetrievalOptions getOptions() {
    return plugin.getDefaultRetrievalOptions()
      .toBuilder()
      .setTimeTravelRequest(getTimeTravelRequest(canonicalKey, context))
      .build();
  }

  @Override
  public String getMetadataLocation() {
    Preconditions.checkNotNull(underlyingTableConfig);
    return underlyingTableConfig.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
  }
}
