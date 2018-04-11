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
package com.dremio.exec.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;

class MetadataRequestOptions {

  private final SchemaConfig schemaConfig;
  private final MetadataStatsCollector statsCollector;
  private final long maxRequestTime;

  private MetadataRequestOptions(
      SchemaConfig schemaConfig,
      MetadataStatsCollector statsCollector,
      long maxRequestTime
  ) {
    this.schemaConfig = checkNotNull(schemaConfig);
    this.statsCollector = checkNotNull(statsCollector);
    this.maxRequestTime = maxRequestTime;
  }

  public MetadataRequestOptions(
      SchemaConfig schemaConfig,
      long maxRequestTime
  ) {
    this(schemaConfig, new MetadataStatsCollector(), maxRequestTime);
  }

  public SchemaConfig getSchemaConfig() {
    return schemaConfig;
  }

  public MetadataStatsCollector getStatsCollector() {
    return statsCollector;
  }

  public long getMaxRequestTime() {
    return maxRequestTime;
  }

  public MetadataRequestOptions cloneWith(String newUser, NamespaceKey newDefaultSchema) {
    SchemaConfig newSchemaConfig = SchemaConfig.newBuilder(newUser)
        .defaultSchema(newDefaultSchema)
        .exposeInternalSources(schemaConfig.exposeInternalSources())
        .setIgnoreAuthErrors(schemaConfig.getIgnoreAuthErrors())
        .optionManager(schemaConfig.getOptions())
        .setViewExpansionContext(schemaConfig.getViewExpansionContext())
        .setDatasetValidityChecker(schemaConfig.getDatasetValidityChecker())
        .build();

    return new MetadataRequestOptions(newSchemaConfig, statsCollector, maxRequestTime);
  }
}
