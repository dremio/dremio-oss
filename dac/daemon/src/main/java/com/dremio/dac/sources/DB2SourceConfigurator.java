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

import static com.dremio.service.namespace.source.proto.SourceType.DB2;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;

/**
 * generates a StoragePluginConfig for DB2
 */
public class DB2SourceConfigurator extends SingleSourceToStoragePluginConfig<DB2Config> {
  public DB2SourceConfigurator() {
    super(DB2);
  }

  @Override
  public StoragePluginConfig configureSingle(DB2Config db2config) {
    String hostname = checkNotNull(db2config.getHostname(), "missing hostname");
    String port = checkNotNull(db2config.getPort(), "missing port");
    String db = checkNotNull(db2config.getDatabaseName(), "missing databaseName");
    Integer fetchSize = db2config.getFetchSize();
    JdbcStorageConfig config = new JdbcStorageConfig(CompatCreator.DB2_DRIVER,
        "jdbc:db2://" + hostname + ":" + port + "/" + db,
        db2config.getUsername(),
        db2config.getPassword(),
        fetchSize != null ? fetchSize : 0 // Using 0 as default to match UI
    );

    return config;
  }
}
