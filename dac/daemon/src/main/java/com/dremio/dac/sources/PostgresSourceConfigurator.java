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

import static com.dremio.service.namespace.source.proto.SourceType.POSTGRES;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.PostgresConfig;
import com.dremio.dac.server.SingleSourceToStoragePluginConfig;
import com.dremio.exec.store.jdbc.CompatCreator;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;

/**
 * generates a StoragePluginConfig from a Postgres Source
 */
public class PostgresSourceConfigurator extends SingleSourceToStoragePluginConfig<PostgresConfig> {

  public PostgresSourceConfigurator() {
    super(POSTGRES);
  }

  @Override
  public StoragePluginConfig configureSingle(PostgresConfig source) {
    PostgresConfig postgres = (PostgresConfig) source;
    String hostname = checkNotNull(postgres.getHostname(), "missing hostname");
    String port = checkNotNull(postgres.getPort(), "missing port");
    String db = checkNotNull(postgres.getDatabaseName(), "missing database");
    // Unfortunate Redshift jdbc driver problem:  Redshift jdbc driver will intercept postgres's connection
    // since redshift registers for both jdbc:postgresql and jdbc:redshift.  So, "OpenSourceSubProtocolOverride=true"
    // indicates to redshift to leave this connection alone!
    // http://stackoverflow.com/questions/31951518/redshift-and-postgres-jdbc-driver-both-intercept-jdbc-postgresql-connection-st
    // http://jira.pentaho.com/browse/PDI-14493
    // http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-options.html
    JdbcStorageConfig config = new JdbcStorageConfig(CompatCreator.POSTGRES_DRIVER,
        "jdbc:postgresql://" + hostname + ":" + port + "/" + db + "?OpenSourceSubProtocolOverride=true",
        postgres.getUsername(),
        postgres.getPassword()
        );

    return config;
  }

}
