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
package com.dremio.exec.record;

import org.apache.curator.shaded.com.google.common.base.Preconditions;

import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.sabot.driver.SchemaChangeMutator;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import io.protostuff.ByteString;

/**
 * Responds to schema change events by merging uninon-ing field lists and individual fields
 */
public class DefaultSchemaMutator implements SchemaChangeMutator {

  @Override
  public DatasetConfig updateForSchemaChange(DatasetConfig oldConfig, BatchSchema expectedSchema, BatchSchema newlyObservedSchema) {
    Preconditions.checkNotNull(oldConfig);
    Preconditions.checkNotNull(newlyObservedSchema);

    BatchSchema newSchema;
    if (DatasetHelper.getSchemaBytes(oldConfig) == null) {
      newSchema = newlyObservedSchema;
    } else {
      newSchema = BatchSchema.fromDataset(oldConfig).merge(newlyObservedSchema);
    }

    DatasetConfig newConfig = clone(oldConfig);
    newConfig.setRecordSchema(ByteString.copyFrom(newSchema.serialize()));

    return newConfig;
  }

  public static DatasetConfig clone(DatasetConfig config) {
    Serializer<DatasetConfig> serializer = ProtostuffSerializer.of(DatasetConfig.getSchema());
    return serializer.deserialize(serializer.serialize(config));
  }

}
