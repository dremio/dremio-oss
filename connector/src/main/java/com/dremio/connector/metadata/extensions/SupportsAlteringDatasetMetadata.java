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
package com.dremio.connector.metadata.extensions;

import java.util.Map;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.AttributeValue;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.options.AlterMetadataOption;

/**
 * Implemented by plugins which can alter dataset metadata
 */
public interface SupportsAlteringDatasetMetadata {

  /**
   * Updates dataset metadata with given attributes
   * Returns new metadata object if attribute values changed else returns passed metadata reference
   * @param datasetHandle
   * @param metadata
   * @param attributes
   * @param options
   * @return
   * @throws ConnectorException
   */
  DatasetMetadata alterMetadata(
      final DatasetHandle datasetHandle, final DatasetMetadata metadata,
      final Map<String, AttributeValue> attributes,
      final AlterMetadataOption... options
  ) throws ConnectorException;

  /**
   * Updates dataset metadata with a given attribute being applied to a given column.
   * Returns new metadata object if attribute values changed else returns passed metadata reference
   * @param datasetHandle
   * @param metadata
   * @param columnName
   * @param attributeName
   * @param attributeValue
   * @param options
   * @return
   * @throws ConnectorException
   */
  default DatasetMetadata alterDatasetSetColumnOption(
    final DatasetHandle datasetHandle, final DatasetMetadata metadata,
    final String columnName,
    final String attributeName, final AttributeValue attributeValue,
    final AlterMetadataOption... options
  ) throws ConnectorException {
    throw new ConnectorException(String.format("Invalid Option [%s]", attributeName));
  }
}
