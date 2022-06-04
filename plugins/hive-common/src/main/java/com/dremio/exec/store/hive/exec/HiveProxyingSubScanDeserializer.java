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
package com.dremio.exec.store.hive.exec;

import java.io.IOException;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.StoragePluginResolver;
import com.dremio.exec.store.SupportsPF4JStoragePlugin;
import com.dremio.exec.store.hive.HiveCommonUtilities;
import com.dremio.exec.store.hive.StoragePluginCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * Helper class for deserializing a SubScan that delegates to a SubScan
 * that is in a separate classloader.
 */
public class HiveProxyingSubScanDeserializer extends StdDeserializer<HiveProxyingSubScan> {
  public HiveProxyingSubScanDeserializer() {
    this(null);
  }

  public HiveProxyingSubScanDeserializer(Class<?> clazz) {
    super(clazz);
  }

  @Override
  public HiveProxyingSubScan deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    final JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    final StoragePluginId pluginId = HiveCommonUtilities.deserialize(jsonParser, deserializationContext,
      node.get("pluginId"), StoragePluginId.class);

    // get subscan class type from plugin.
    final StoragePluginResolver storagePluginResolver = (StoragePluginResolver) deserializationContext
      .findInjectableValue(StoragePluginResolver.class.getName(), null, null);
    final SupportsPF4JStoragePlugin pf4JStoragePlugin = storagePluginResolver.getSource(pluginId);
    final StoragePluginCreator.PF4JStoragePlugin plugin = pf4JStoragePlugin.getPF4JStoragePlugin();
    final Class<? extends HiveProxiedSubScan> scanClazz = plugin.getSubScanClass();

    HiveProxiedSubScan scan = HiveCommonUtilities.deserialize(jsonParser, deserializationContext, node.get("wrappedHiveScan"), scanClazz);
    return new HiveProxyingSubScan(pluginId, scan);
  }
}
