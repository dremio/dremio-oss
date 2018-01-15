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


import static com.dremio.service.namespace.source.proto.SourceType.MSSQL;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.MSSQLConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;


/**
 * generates a StoragePluginConfig from a MSSQL Source
 *
 */
public class MSSQLSourceConfigurator extends SingleSourceToStoragePluginConfig<MSSQLConfig> {

  public MSSQLSourceConfigurator() {
    super(MSSQL);
  }

  @Override
  public StoragePluginConfig configureSingle(MSSQLConfig mssql) {
    final String hostname = checkNotNull(mssql.getHostname(), "missing hostname");
    final Integer fetchSize = mssql.getFetchSize();

    final StringBuilder urlBuilder = new StringBuilder("jdbc:sqlserver://").append(hostname);
    if (mssql.getPort() != null) {
      urlBuilder.append(":").append(mssql.getPort());
    }

    if (mssql.getDatabase() != null) {
      urlBuilder.append(";databaseName=").append(mssql.getDatabase());
    }

    final JdbcStorageConfig config = new JdbcStorageConfig(CompatCreator.MSSQL_DRIVER,
        urlBuilder.toString(),
        mssql.getUsername(),
        mssql.getPassword(),
        fetchSize != null ? fetchSize : 0, // Using 0 as default to match UI
        mssql.getDatabase(),
        mssql.getShowOnlyConnectionDatabase() != null ? mssql.getShowOnlyConnectionDatabase() : false
        );
    return config;
  }

}
