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

import com.dremio.ValidatingGnarlyStyle;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.service.namespace.NamespaceKey;
import java.util.Map;
import org.immutables.value.Value;

/** Metadata request options. */
@Value.Immutable
@ValidatingGnarlyStyle
public abstract class MetadataRequestOptions {

  public abstract SchemaConfig getSchemaConfig();

  @Value.Default
  public MetadataStatsCollector getStatsCollector() {
    return new MetadataStatsCollector();
  }

  @Value.Default
  public CaseInsensitiveMap<VersionContext> getSourceVersionMapping() {
    return CaseInsensitiveMap.newHashMap();
  }

  /**
   * Consider the metadata valid only if it is newer than the given time.
   *
   * <p>By default, there is no lower bound.
   */
  @Value.Default
  public long newerThan() {
    return Long.MAX_VALUE;
  }

  /**
   * If metadata validity should be checked. If set to false, -Completeness of the datasetConfig is
   * still checked (see ManagedStoragePlugin#isComplete) -Validity - i.e freshness of metadata
   * -schema, splits , partition info based on metadata policy is *not* checked. -newerThan() has no
   * effect if this is set to false Change default to false when only if the following cases are
   * desired : - Cached version of a table or view from Dremio KV store that are complete - Inline
   * refresh will occur if DatasetConfig is incomplete - Table from an External catalog - Nessie
   * will be returned since metadata is always up to date
   *
   * <p>By default, the validity is checked.
   */
  @Value.Default
  public boolean checkValidity() {
    return true;
  }

  /** See {@link MetadataRequestOptions#checkValidity()}. Specifically for Iceberg datasets. */
  @Value.Default
  public boolean checkIcebergValidity() {
    return checkValidity();
  }

  /**
   * If this flag is set to true, - A null entry or shallow entry will not cause a promotion/inline
   * refresh to occur Change default to true only if the following behavior is desired : - Get only
   * cached version of a table from Dremio KV store - Get table from an External catalog - Nessie
   * (These will be returned since they don't go through promotion) Note : By default, a null or
   * shallow entry in Namepsace is considered "incomplete" and promotion will be attempted. Tables
   * in Nessie are considered always valid and uptodate and promotion/refresh does not apply to them
   *
   * <p>
   */
  @Value.Default
  public boolean neverPromote() {
    return false;
  }

  /**
   * If set to true and any versioned dataset is resolved using the default source version, then a
   * validation exception is thrown. This flag can be used to validate that the plan associated to a
   * reflection refresh job or a view definition contains only table references that resolve using
   * the AT SQL syntax or a source version mapping explicitly set by the caller.
   *
   * @return
   */
  @Value.Default
  public boolean errorOnUnspecifiedSourceVersion() {
    return false;
  }

  /**
   * If set to true, request Catalog to use caching namespace to reduce duplicated calls to KV
   * store.
   *
   * @return
   */
  @Value.Default
  public boolean useCachingNamespace() {
    return false;
  }

  /**
   * If set to true, request Catalog to use internal metadata table of Unlimited Splits dataset for
   * retrieving TableMetadata.
   *
   * @return
   */
  @Value.Default
  public boolean useInternalMetadataTable() {
    return false;
  }

  MetadataRequestOptions cloneWith(
      CatalogIdentity subject, NamespaceKey newDefaultSchema, boolean checkValidity) {
    final SchemaConfig newSchemaConfig =
        SchemaConfig.newBuilder(subject)
            .defaultSchema(newDefaultSchema)
            .exposeInternalSources(getSchemaConfig().exposeInternalSources())
            .setIgnoreAuthErrors(getSchemaConfig().getIgnoreAuthErrors())
            .optionManager(getSchemaConfig().getOptions())
            .setViewExpansionContext(getSchemaConfig().getViewExpansionContext())
            .setDatasetValidityChecker(getSchemaConfig().getDatasetValidityChecker())
            .build();

    return new ImmutableMetadataRequestOptions.Builder()
        .from(this)
        .setSchemaConfig(newSchemaConfig)
        .setCheckValidity(checkValidity)
        .build();
  }

  MetadataRequestOptions cloneWith(Map<String, VersionContext> sourceVersionMapping) {
    return new ImmutableMetadataRequestOptions.Builder()
        .from(this)
        .setSourceVersionMapping(CaseInsensitiveMap.newImmutableMap(sourceVersionMapping))
        .build();
  }

  MetadataRequestOptions cloneWith(String sourceName, VersionContext versionContext) {
    Map<String, VersionContext> sourceVersionMapping = CaseInsensitiveMap.newHashMap();
    sourceVersionMapping.putAll(this.getSourceVersionMapping());
    sourceVersionMapping.put(sourceName, versionContext);
    return new ImmutableMetadataRequestOptions.Builder()
        .from(this)
        .setSourceVersionMapping(CaseInsensitiveMap.newImmutableMap(sourceVersionMapping))
        .build();
  }

  /**
   * Create a new builder.
   *
   * @return new builder
   */
  public static ImmutableMetadataRequestOptions.Builder newBuilder() {
    return new ImmutableMetadataRequestOptions.Builder();
  }

  /**
   * Create a new options instance with the given schema config.
   *
   * @param schemaConfig schema config
   * @return metadata request options
   */
  public static MetadataRequestOptions of(SchemaConfig schemaConfig) {
    return newBuilder().setSchemaConfig(schemaConfig).build();
  }

  /**
   * Create a new options instance with the given schema config and sourceVersionMapping.
   *
   * @param schemaConfig schema config
   * @param sourceVersionMapping source version map
   * @return metadata request options
   */
  public static MetadataRequestOptions of(
      SchemaConfig schemaConfig, Map<String, VersionContext> sourceVersionMapping) {
    return newBuilder()
        .setSchemaConfig(schemaConfig)
        .setSourceVersionMapping(CaseInsensitiveMap.newImmutableMap(sourceVersionMapping))
        .build();
  }

  public VersionContext getVersionForSource(String sourceName, NamespaceKey key) {
    VersionContext versionContext = getSourceVersionMapping().get(sourceName);
    if (versionContext == null) {
      if (this.errorOnUnspecifiedSourceVersion()) {
        throw UserException.validationError()
            .message(
                String.format(
                    "Version context for table %s must be specified using AT SQL syntax",
                    key.toString()))
            .build();
      }
      versionContext = VersionContext.NOT_SPECIFIED;
    }
    return versionContext;
  }
}
