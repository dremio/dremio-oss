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
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.SourceMetadata;

/**
 * This is an optional interface. When extended by an implementation of {@link SourceMetadata}, this is used to provide
 * a list of dataset handles.
 */
public interface SupportsListingDatasets {

  /**
   * Returns a listing of dataset handles, where each handle represents a dataset in the source.
   *
   * @param options options
   * @return listing of dataset handles, not null
   */
  DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException;

}
