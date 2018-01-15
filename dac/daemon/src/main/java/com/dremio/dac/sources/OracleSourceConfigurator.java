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

import static com.dremio.service.namespace.source.proto.SourceType.ORACLE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.OracleConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;

/**
 * generates a StoragePluginConfig from an Oracle Source
 *
 */
public class OracleSourceConfigurator extends SingleSourceToStoragePluginConfig<OracleConfig> {

  public OracleSourceConfigurator() {
    super(ORACLE);
  }

  @Override
  public StoragePluginConfig configureSingle(OracleConfig oracle) {
    final String username = checkNotNull(oracle.getUsername(), "missing username");
    final String password = checkNotNull(oracle.getPassword(), "missing password");
    final String hostname = checkNotNull(oracle.getHostname(), "missing hostname");
    final String port = checkNotNull(oracle.getPort(), "missing port");
    final String instance = checkNotNull(oracle.getInstance(), "missing instance");
    final Integer fetchSize = oracle.getFetchSize();

    final JdbcStorageConfig config = new JdbcStorageConfig(CompatCreator.ORACLE_DRIVER,
        "jdbc:oracle:thin:" + username + "/" + password + "@" + hostname + ":"
            + port + "/" + instance,
        username,
        password,
        fetchSize != null ? fetchSize : 0, // Using 0 as default to match UI
        null,
        false
        );

    return config;
  }

}
