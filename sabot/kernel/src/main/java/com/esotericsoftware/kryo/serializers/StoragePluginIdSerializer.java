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
package com.esotericsoftware.kryo.serializers;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.serialization.kryo.InjectionMapping;
import com.dremio.exec.planner.serialization.kryo.serializers.SourceConfigAwareConnectionConfDeserializer;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;

/**
 * Kryo serializer/deserializer with specialized code for handling deserialization of Hive plugin
 * configurations from versions older than 4.0.0
 */
public class StoragePluginIdSerializer extends InjectingSerializer<StoragePluginId> {
  public StoragePluginIdSerializer(Kryo kryo, InjectionMapping mapping) {
    super(kryo, StoragePluginId.class, mapping);
  }

  @Override
  public StoragePluginId read(Kryo kryo, Input input, Class<StoragePluginId> type) {
    // Override read such that we construct a specialized serializer for the ConnectionConf
    // based on the value (not metadata) of the SourceConfig. Aside from the reading from fields,
    // this code is the same as the base class implementation of read().
    try {
      if (config.isOptimizedGenerics()) {
        if (typeParameters != null && getGenerics() != null) {
          // Rebuild fields info. It may result in rebuilding the
          // genericScope
          rebuildCachedFields();
        }

        if (getGenericsScope() != null) {
          // Push a new scope for generics
          kryo.getGenericsResolver().pushScope(type, getGenericsScope());
        }
      }

      final StoragePluginId object = create(kryo, input, type);
      kryo.reference(object);

      // Read fields up to config. Note that fields are serialized in _alphabetical_ order,
      // not the order they are declared in!
      // Using indices rather than field name lookups for performance reasons.
      final CachedField[] fields = this.getFields();
      fields[StoragePluginId.CAPABILITIES_INDEX_0].read(input, object); // capabilities
      fields[StoragePluginId.CONFIG_INDEX_1].read(input, object); // config

      // Add custom serializer for the ConnectionConf that is aware of the SourceConfig.
      fields[StoragePluginId.CONNECTION_INDEX_2].setSerializer(
          getConnectionConfDeserializer(object.getConfig()));
      fields[StoragePluginId.CONNECTION_INDEX_2].read(input, object); // connection
      fields[StoragePluginId.HASH_CODE_INDEX_3].read(input, object); // hashCode

      // De-serialize transient fields
      if (config.isSerializeTransient()) {
        for (int i = 0, n = getTransientFields().length; i < n; i++) {
          getTransientFields()[i].read(input, object);
        }
      }
      return object;
    } finally {
      if (config.isOptimizedGenerics()
          && getGenericsScope() != null
          && kryo.getGenericsResolver() != null) {
        // Pop the scope for generics
        kryo.getGenericsResolver().popScope();
      }
    }
  }

  protected Serializer getConnectionConfDeserializer(SourceConfig config) {
    return new SourceConfigAwareConnectionConfDeserializer(config);
  }
}
