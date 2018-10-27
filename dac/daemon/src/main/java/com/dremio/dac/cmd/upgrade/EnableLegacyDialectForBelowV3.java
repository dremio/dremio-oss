/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.cmd.upgrade;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.jdbc.conf.MSSQLConf;
import com.dremio.exec.store.jdbc.conf.PostgresConf;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * When upgrading, this will enable legacy dialect for connection configurations made prior to Dremio 3.0.0.
 */
public class EnableLegacyDialectForBelowV3 extends UpgradeTask {
  public EnableLegacyDialectForBelowV3() {
    super("Enable legacy dialect for connection configurations made prior to Dremio 3.0.0.", VERSION_106, VERSION_300, NORMAL_ORDER);
  }

  @Override
  public void upgrade(final UpgradeContext context) {
    final NamespaceService namespaceService = new NamespaceServiceImpl(context.getKVStoreProvider());

    for (final SourceConfig sourceConfig : namespaceService.getSources()) {
      try {
        final ConnectionConf<?, ?> connectionConf = context.getConnectionReader().getConnectionConf(sourceConfig);

        if (null == connectionConf || connectionConf.isInternal()) {
          continue;
        }

        if (connectionConf instanceof PostgresConf) {
          final PostgresConf postgresConf = (PostgresConf) connectionConf;
          postgresConf.useLegacyDialect = true;

          updateConnectionConf(sourceConfig, postgresConf, namespaceService);
        } else if (connectionConf instanceof MSSQLConf) {
          final MSSQLConf mssqlConf = (MSSQLConf) connectionConf;
          mssqlConf.useLegacyDialect = true;

          updateConnectionConf(sourceConfig, mssqlConf, namespaceService);
        }
      } catch (final Exception e) {
        System.out.printf("  Could not get connection config of source [%s] with exception [%s]\n", sourceConfig.getName(), e);
      }
    }
  }

  private void updateConnectionConf(
    final SourceConfig sourceConfig,
    final AbstractConnectionConf connectionConf,
    final NamespaceService namespaceService) throws NamespaceException {
    sourceConfig.setConnectionConf(connectionConf);

    namespaceService.addOrUpdateSource(sourceConfig.getKey(), sourceConfig);

    System.out.printf("  Enabled legacy dialect for connection config of source [%s]\n", sourceConfig.getName());
  }
}
