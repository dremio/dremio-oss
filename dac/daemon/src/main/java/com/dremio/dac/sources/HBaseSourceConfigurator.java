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

import static com.dremio.service.namespace.source.proto.SourceType.HBASE;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.HBaseConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.hbase.HBaseStoragePluginConfig;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

/**
 * generates a StoragePluginConfig for a Apache HBase Source
 */
public class HBaseSourceConfigurator extends SingleSourceToStoragePluginConfig<HBaseConfig> {

  public HBaseSourceConfigurator() {
    super(HBASE);
  }

  @Override
  public StoragePluginConfig configureSingle(HBaseConfig sourceConfig) {
    String zkQuorum = checkNotNull(sourceConfig.getZkQuorum(), "Zookeeper Quorum is missing");
    Integer zkPort = Optional.fromNullable(sourceConfig.getPort()).or(Integer.valueOf(2181));
    List<Property> properties = Optional.fromNullable(sourceConfig.getPropertyList()).or(new ArrayList<Property>());
    boolean isRegionSizeCalculated = Optional.fromNullable(sourceConfig.getIsSizeCalcEnabled()).or(Boolean.FALSE);

    final Map<String, String> config = Maps.newHashMap();
    config.put("hbase.zookeeper.quorum", zkQuorum);
    config.put("hbase.zookeeper.property.clientPort", Integer.toString(zkPort));
    for (Property property : properties) {
      config.put(property.getName(), property.getValue());
    }

    final HBaseStoragePluginConfig pluginConfig = new HBaseStoragePluginConfig(config, isRegionSizeCalculated);
    return pluginConfig;
  }
}
