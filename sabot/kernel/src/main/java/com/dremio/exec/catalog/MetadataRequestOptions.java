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
package com.dremio.exec.catalog;

import org.immutables.value.Value;

import com.dremio.ValidatingGnarlyStyle;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;

/**
 * Metadata request options.
 */
@Value.Immutable
@ValidatingGnarlyStyle
public abstract class MetadataRequestOptions {

  public abstract SchemaConfig getSchemaConfig();

  public abstract MetadataStatsCollector getStatsCollector();

  /**
   * Consider the metadata valid only if it is newer than the given time.
   * <p>
   * By default, there is no lower bound.
   */
  @Value.Default
  public long newerThan() {
    return Long.MAX_VALUE;
  }

  /**
   * If metadata validity should be checked.
   * <p>
   * By default, the validity is checked.
   */
  @Value.Default
  public boolean checkValidity() {
    return true;
  }

  MetadataRequestOptions cloneWith(String newUser, NamespaceKey newDefaultSchema) {
    final SchemaConfig newSchemaConfig = SchemaConfig.newBuilder(newUser)
        .defaultSchema(newDefaultSchema)
        .exposeInternalSources(getSchemaConfig().exposeInternalSources())
        .setIgnoreAuthErrors(getSchemaConfig().getIgnoreAuthErrors())
        .optionManager(getSchemaConfig().getOptions())
        .setViewExpansionContext(getSchemaConfig().getViewExpansionContext())
        .setDatasetValidityChecker(getSchemaConfig().getDatasetValidityChecker())
        .build();

    return new ImmutableMetadataRequestOptions.Builder().from(this)
        .setSchemaConfig(newSchemaConfig)
        .build();
  }

  /**
   * Create a new builder.
   *
   * @return new builder
   */
  public static ImmutableMetadataRequestOptions.Builder newBuilder() {
    return new ImmutableMetadataRequestOptions.Builder()
        .setStatsCollector(new MetadataStatsCollector());
  }

  /**
   * Create a new builder.
   *
   * @param schemaConfig schema config
   * @return new builder
   */
  public static ImmutableMetadataRequestOptions.Builder newBuilder(SchemaConfig schemaConfig) {
    return new ImmutableMetadataRequestOptions.Builder()
        .setSchemaConfig(schemaConfig)
        .setStatsCollector(new MetadataStatsCollector());
  }

  /**
   * Create a new options instance with the given schema config.
   *
   * @param schemaConfig schema config
   * @return metadata request options
   */
  public static MetadataRequestOptions of(SchemaConfig schemaConfig) {
    return newBuilder(schemaConfig).build();
  }
}
