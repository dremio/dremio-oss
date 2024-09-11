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
package com.dremio.exec.store.dfs.system;

import static com.dremio.exec.store.dfs.system.SystemIcebergTablesStoragePluginConfig.SYSTEM_ICEBERG_TABLES_PLUGIN_NAME;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.Service;
import com.dremio.service.coordinator.proto.DataCredentials;
import com.dremio.service.namespace.source.proto.SourceConfig;
import java.net.URI;

/**
 * Factory class for creating SystemIcebergTablesStoragePluginConfig and SourceConfig objects. This
 * class is responsible for creating the configuration objects required by the Iceberg tables
 * storage plugin. It implements the Service interface, which allows it to be managed by the Dremio
 * service lifecycle.
 */
public class SystemIcebergTablesStoragePluginConfigFactory implements Service {

  @Override
  public void start() throws Exception {}

  @Override
  public void close() throws Exception {}

  /**
   * Creates a new {@link ConnectionConf} object for the SystemIcebergTablesStoragePluginConfig.
   *
   * @param path The URI path for the storage plugin configuration.
   * @param enableAsync The flag to enable asynchronous operations.
   * @param enableS3FileStatusCheck The flag to enable S3 file status check.
   * @param dataCredentials The data credentials for authentication.
   * @return The new ConnectionConf object.
   */
  public ConnectionConf<?, ?> create(
      URI path,
      boolean enableAsync,
      boolean enableS3FileStatusCheck,
      DataCredentials dataCredentials) {
    return new SystemIcebergTablesStoragePluginConfig(
        path, enableAsync, enableS3FileStatusCheck, dataCredentials);
  }

  /**
   * Creates a new system Iceberg table plugin source {@link SourceConfig} object with the given
   * {@link ConnectionConf}.
   *
   * @param connectionConf The {@link ConnectionConf} object used for configuring the source.
   * @return The new {@link SourceConfig} object.
   */
  public static SourceConfig create(ConnectionConf<?, ?> connectionConf) {
    SourceConfig conf = new SourceConfig();
    conf.setConnectionConf(connectionConf);
    conf.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    conf.setName(SYSTEM_ICEBERG_TABLES_PLUGIN_NAME);
    return conf;
  }
}
