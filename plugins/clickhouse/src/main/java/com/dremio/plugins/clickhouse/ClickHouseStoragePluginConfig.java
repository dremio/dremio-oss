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
package com.dremio.plugins.clickhouse;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.catalog.conf.SourceType;
import io.protostuff.Tag;
import javax.inject.Provider;

/** Configuration for ClickHouse source. */
@SourceType(value = "CLICKHOUSE", label = "ClickHouse", uiConfig = "clickhouse-storage-layout.json")
public class ClickHouseStoragePluginConfig
    extends ConnectionConf<ClickHouseStoragePluginConfig, ClickHouseStoragePlugin> {

  @Tag(1)
  @DisplayMetadata(label = "Host")
  public String hostname = "localhost";

  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 8123;

  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database = "default";

  @Tag(4)
  @DisplayMetadata(label = "Username")
  public String username = "default";

  @Tag(5)
  @Secret
  @DisplayMetadata(label = "Password")
  public SecretRef password = SecretRef.EMPTY;

  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Use SSL")
  public boolean useSsl = false;

  @Override
  public ClickHouseStoragePlugin newPlugin(
      PluginSabotContext pluginSabotContext,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new ClickHouseStoragePlugin(this, pluginSabotContext, name);
  }
}
