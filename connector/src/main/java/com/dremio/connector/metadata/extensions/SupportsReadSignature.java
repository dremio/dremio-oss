/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.connector.metadata.extensions;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.SourceMetadata;

/**
 * This is an optional interface. When extended by an implementation of {@link SourceMetadata}, this is used to provide
 * and check read signatures. A read signature holds information about dataset metadata that can be quickly computed
 * to check metadata validity, rather than a full metadata comparison.
 */
public interface SupportsReadSignature {

  /**
   * Provide a read signature for the dataset. This is invoked only if dataset metadata is available for a
   * dataset.
   *
   * @param datasetHandle dataset handle
   * @param metadata      dataset metadata
   * @return read signature, not null
   */
  BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) throws ConnectorException;

  /**
   * Based on the given signature, check if the given metadata is valid for the dataset according to the
   * source. If the given metadata is still valid and the catalog need not be updated, return
   * {@link MetadataValidity#VALID}. If not, return {@link MetadataValidity#INVALID}.
   *
   * @param signature     read signature
   * @param datasetHandle dataset handle
   * @param metadata      dataset metadata
   * @param options       options
   * @return state of metadata, not null
   */
  MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options
  ) throws ConnectorException;

  /**
   * State of metadata.
   */
  enum MetadataValidity {
    VALID,

    INVALID
  }
}
