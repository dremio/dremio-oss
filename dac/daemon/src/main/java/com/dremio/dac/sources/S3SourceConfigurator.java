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

import static com.dremio.dac.server.NASSourceConfigurator.getDefaultFormats;
import static com.dremio.service.namespace.source.proto.SourceType.S3;

import java.util.List;
import java.util.Map;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.plugins.s3.store.S3PluginConfig;
import com.google.common.collect.Maps;

/**
 * generates a StoragePluginConfig from an S3 Source
 *
 */
public class S3SourceConfigurator extends SingleSourceToStoragePluginConfig<S3Config> {

  public S3SourceConfigurator() {
    super(S3);
  }

  @Override
  public StoragePluginConfig configureSingle(S3Config s3) {
    Map<String, String> properties = null;
    final List<Property> propertyList = s3.getPropertyList();
    if (propertyList != null && !propertyList.isEmpty()) {
      properties = Maps.newHashMap();
      for (Property p : s3.getPropertyList()) {
        properties.put(p.getName(), p.getValue());
      }
    }

    S3PluginConfig config = new S3PluginConfig(s3.getAccessKey(), s3.getAccessSecret(), s3.getSecure(),
        s3.getExternalBucketList(), properties, getDefaultFormats());

    return config;
  }

}
