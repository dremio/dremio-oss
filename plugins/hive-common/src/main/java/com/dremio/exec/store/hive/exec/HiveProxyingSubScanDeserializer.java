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

import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.hive.StoragePluginCreator;
import com.dremio.exec.store.hive.proxy.HiveProxiedSubScan;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.introspect.BasicBeanDescription;

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

    final String pluginName = node.get("pluginName").asText();
    final CatalogService catalogService = (CatalogService) deserializationContext
      .findInjectableValue(CatalogService.class.getName(), null, null);

    final StoragePluginCreator.PF4JStoragePlugin plugin = catalogService.getSource(pluginName);
    final Class<? extends HiveProxiedSubScan> scanClazz = plugin.getSubScanClass();

    final JavaType scanType = deserializationContext.getTypeFactory().constructType(scanClazz);
    final BasicBeanDescription description = deserializationContext.getConfig().introspect(scanType);
    final JsonDeserializer<Object> subScanDeserializer = deserializationContext.getFactory().createBeanDeserializer(
      deserializationContext, scanType, description);

    if (subScanDeserializer instanceof ResolvableDeserializer) {
      ((ResolvableDeserializer) subScanDeserializer).resolve(deserializationContext);
    }

    final JsonParser movedParser = jsonParser.getCodec().treeAsTokens(node.get("wrappedHiveScan"));
    deserializationContext.getConfig().initialize(movedParser);

    if (movedParser.getCurrentToken() == null) {
      movedParser.nextToken();
    }

    final HiveProxiedSubScan scan = (HiveProxiedSubScan) subScanDeserializer.deserialize(movedParser, deserializationContext);
    return new HiveProxyingSubScan(pluginName, scan);
  }
}
