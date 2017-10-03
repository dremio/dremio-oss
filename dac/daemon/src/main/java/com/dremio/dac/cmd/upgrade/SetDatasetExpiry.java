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
package com.dremio.dac.cmd.upgrade;

import java.util.concurrent.TimeUnit;

import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Throwables;

/**
 * For each source, upgrade its MetadataPolicy deprecated 'dataset_definition_ttl_ms' field
 * --> dataset_definition_refresh_after_ms set to dataset_definition_ttl_ms
 * --> dataset_definition_expire_after_ms set to:
 *     - CatalogService.DEFAULT_EXPIRE_MILLIS, if dataset_definition_ttl_ms is at its old default (30 minutes)
 *     - 3 * dataset_definition_ttl_ms, if not
 */
public class SetDatasetExpiry extends UpgradeTask {
  private static final long ORIGINAL_DEFAULT_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(30, TimeUnit.MINUTES);

  SetDatasetExpiry() {
    super("Setting dataset expiry", VERSION_106, VERSION_111);
  }

  @Override
  public void upgrade(UpgradeContext context) {
    final NamespaceService namespace = new NamespaceServiceImpl(context.getKVStoreProvider().get());

    for (SourceConfig sourceConfig : namespace.getSources()) {
      MetadataPolicy metadataPolicy = sourceConfig.getMetadataPolicy();
      if (metadataPolicy != null) {
        long originalTTL = metadataPolicy.getDatasetDefinitionTtlMs();
        metadataPolicy.setDatasetDefinitionRefreshAfterMs(originalTTL);
        if (originalTTL == ORIGINAL_DEFAULT_REFRESH_MILLIS) {
          metadataPolicy.setDatasetDefinitionExpireAfterMs(CatalogService.DEFAULT_EXPIRE_MILLIS);
        } else {
          metadataPolicy.setDatasetDefinitionExpireAfterMs(3 * originalTTL);
        }
        sourceConfig.setMetadataPolicy(metadataPolicy);
        System.out.println("  Updating source " + sourceConfig.getName());
        try {
          namespace.addOrUpdateSource(new NamespaceKey(sourceConfig.getName()), sourceConfig);
        } catch (NamespaceException e) {
          Throwables.propagate(e);
        }
      }
    }
  }
}
