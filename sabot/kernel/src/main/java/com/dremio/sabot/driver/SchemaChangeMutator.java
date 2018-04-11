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
package com.dremio.sabot.driver;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.DefaultSchemaMutator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * Interface for responding to schema change.
 */
public interface SchemaChangeMutator {

  SchemaChangeMutator DEFAULT = new DefaultSchemaMutator();

  /**
   * Update an existing DatasetConfig based on the information provided by a newly observed schema.
   * @param oldConfig
   * @param newlyObservedSchema
   * @return A cloned, updated version of the DatasetConfig.
   */
  DatasetConfig updateForSchemaChange(DatasetConfig oldConfig, BatchSchema expectedSchema, BatchSchema newlyObservedSchema);
}
