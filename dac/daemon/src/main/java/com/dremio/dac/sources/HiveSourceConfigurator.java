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
package com.dremio.dac.sources;

import static com.dremio.service.namespace.source.proto.SourceType.HIVE;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.dremio.dac.proto.model.source.HiveConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.hive.HiveStoragePluginConfig;
import com.google.common.collect.Maps;

/**
 * generates a StoragePluginConfig from a Apache Hive Source
 */
public class HiveSourceConfigurator extends SingleSourceToStoragePluginConfig<HiveConfig> {

  public HiveSourceConfigurator() {
    super(HIVE);
  }

  @Override
  public HiveStoragePluginConfig configureSingle(HiveConfig hive) {
    final String metastoreURI =
        String.format("thrift://%s:%d", checkNotNull(hive.getHostname()), hive.getPort());

    final Map<String, String> configProps = Maps.newHashMap();
    configProps.put("hive.metastore.uris", metastoreURI);
    for(Property p : hive.getPropertyList()) {
      configProps.put(p.getName(), p.getValue());
    }

    if (hive.getEnableSasl()) {
      configProps.put("hive.metastore.sasl.enabled", "true");
      configProps.put("hive.metastore.kerberos.principal", checkNotNull(hive.getKerberosPrincipal()));
    }

    final HiveStoragePluginConfig config = new HiveStoragePluginConfig(configProps);
    return config;
  }
}
