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
package com.dremio.exec.planner.serialization.kryo.serializers;

import com.dremio.service.namespace.source.proto.SourceConfig;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.ExternalizableSerializer;

/**
 * A deserializer for ConnectionConfs that is aware of the SourceConfig associated with it.
 * Generally deserializing a field is independent of the context around the field, however for Hive
 * plugin configurations, in versions older than 4.0.0, the same field value can represent either
 * Hive 2 or Hive 3 connections. Starting with 4.0.0, these are distinct values with different
 * types, but when upgrading from versions lower than 4.0.0, we need to use the SourceConfig to
 * identify the target source type before deserializing the Hive plugin configuration.
 */
public class SourceConfigAwareConnectionConfDeserializer extends ExternalizableSerializer {
  public static final String HIVE2_SOURCE_TYPE = "HIVE";
  public static final String HIVE3_SOURCE_TYPE = "HIVE3";

  private static final String HIVE2_CONF_FQDN =
      "com.dremio.exec.store.hive.Hive2StoragePluginConfig";
  private static final String HIVE3_CONF_FQDN =
      "com.dremio.exec.store.hive.Hive3StoragePluginConfig";

  private final SourceConfig config;

  public SourceConfigAwareConnectionConfDeserializer(SourceConfig config) {
    this.config = config;
  }

  public SourceConfig getConfig() {
    return config;
  }

  @Override
  public Object read(Kryo kryo, Input input, Class type) {
    try {
      if (HIVE2_SOURCE_TYPE.equals(config.getType())) {
        return super.read(kryo, input, Class.forName(HIVE2_CONF_FQDN));
      } else if (HIVE3_SOURCE_TYPE.equals(config.getType())) {
        return super.read(kryo, input, Class.forName(HIVE3_CONF_FQDN));
      } else {
        return super.read(kryo, input, type);
      }
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }
}
