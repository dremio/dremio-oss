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
package com.dremio.exec.store;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.VersionedDatasetAdapter;
import com.dremio.exec.catalog.VersionedPlugin;

public interface VersionedDatasetHandle extends DatasetHandle {
  /**
   * @return Type of entity - IcebergView, IcebergTable etc
   */
  VersionedPlugin.EntityType getType();

  /**
   * @param vda            Adapter that provides methods to translate from external versioned type to DremioTable
   * @param accessUserName UserName used to access the table
   */
  DremioTable translateToDremioTable(VersionedDatasetAdapter vda, String accessUserName);

  /**
   * A unique ID that represents a unique snapshot of a versioned table or view
   * @return
   */
  String getUniqueInstanceId() ;

  /**
   * A stable entity id that remains constant for the lifetime of a versioned object (until it is dropped)
   * @return
   */
  String getContentId();

}
