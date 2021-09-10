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
package com.dremio.exec.store.metadatarefresh;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;

/**
 * Dummy handle used for new metadata refresh flows. Returns only path and type of dataset.
 * Other operations such as creating partition chunks, readSignature creation etc are not supported
 * as they are done during execution.
 */
public class UnlimitedSplitsFileDatasetHandle implements FileDatasetHandle {

  DatasetType type;
  NamespaceKey path;

  public UnlimitedSplitsFileDatasetHandle(DatasetType type, NamespaceKey datasetPath) {
    this.type = type;
    this.path  = datasetPath;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(GetMetadataOption... options) throws ConnectorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PartitionChunkListing listPartitionChunks(ListPartitionChunkOption... options) throws ConnectorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BytesOutput provideSignature(DatasetMetadata metadata) throws ConnectorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatasetType getDatasetType() {
    return type;
  }

  @Override
  public EntityPath getDatasetPath() {
    return MetadataObjectsUtils.toEntityPath(path);
  }
}
