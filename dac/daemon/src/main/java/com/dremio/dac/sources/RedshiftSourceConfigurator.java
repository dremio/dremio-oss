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

import static com.dremio.service.namespace.source.proto.SourceType.REDSHIFT;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.RedshiftConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;

/**
 * generates a StoragePluginConfig from a Redshift Source
 */
public class RedshiftSourceConfigurator extends SingleSourceToStoragePluginConfig<RedshiftConfig> {

  public RedshiftSourceConfigurator() {
    super(REDSHIFT);
  }

  @Override
  public StoragePluginConfig configureSingle(RedshiftConfig redshiftConfig) {
    final String connectionString = checkNotNull(redshiftConfig.getConnectionString(), "missing connection URL");
    final Integer fetchSize = redshiftConfig.getFetchSize();

    final JdbcStorageConfig config = new JdbcStorageConfig(
        CompatCreator.REDSHIFT_DRIVER,
        connectionString,
        redshiftConfig.getUsername(),
        redshiftConfig.getPassword(),
        fetchSize != null ? fetchSize : 0, // Using 0 as default to match UI
        null,
        false
    );
    return config;
  }
}
