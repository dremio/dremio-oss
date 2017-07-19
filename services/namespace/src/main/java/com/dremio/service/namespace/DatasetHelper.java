/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.service.namespace;

import com.dremio.service.namespace.dataset.proto.DatasetConfig;

import io.protostuff.ByteString;

/**
 * Dataset definition helper.
 */
public final class DatasetHelper {
  public static final int NO_VERSION = 0;
  public static final int CURRENT_VERSION = 1;

  private DatasetHelper(){}

  /**
   * Retrieve the schema bytes for a config. Manages different locations due to legacy storage.
   * @param config DatasetConfig to view.
   * @return ByteString for schema if property found. Otherwise null.
   */
  public static ByteString getSchemaBytes(DatasetConfig config){
    ByteString recordSchema = config.getRecordSchema();
    if(recordSchema == null && config.getPhysicalDataset() != null){
      recordSchema = config.getPhysicalDataset().getDeprecatedDatasetSchema();
    }
    return recordSchema;
  }
}
