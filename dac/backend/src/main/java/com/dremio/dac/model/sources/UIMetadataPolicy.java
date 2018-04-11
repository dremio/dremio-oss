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
package com.dremio.dac.model.sources;

import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class that exposes metadata refresh policy information to REST APIs.
 */
public class UIMetadataPolicy {

  public static final UIMetadataPolicy DEFAULT_UIMETADATA_POLICY = UIMetadataPolicy.of(CatalogService.DEFAULT_METADATA_POLICY);

  private final UIUpdateMode updateMode;
  private final long namesRefreshMillis;
  private final long authTTLMillis;
  private final long datasetDefinitionRefreshAfterMillis;
  private final long datasetDefinitionExpireAfterMillis;

  /**
   * Enumeration describing the types of updates available.
   */
  enum UIUpdateMode {
    PREFETCH,
    PREFETCH_QUERIED,
    INLINE;

    public static UIUpdateMode of(UpdateMode mode){
      switch(mode){
      case INLINE:
        return INLINE;
      case PREFETCH:
        return PREFETCH;
      case PREFETCH_QUERIED:
      case UNKNOWN:
      default:
        return PREFETCH_QUERIED;
      }
    }

    public UpdateMode asUpdateMode(){
      switch(this){
      case INLINE:
        return UpdateMode.INLINE;
      case PREFETCH:
        return UpdateMode.PREFETCH;
      case PREFETCH_QUERIED:
      default:
        return UpdateMode.PREFETCH_QUERIED;
      }
    }
  }

  @JsonCreator
  public UIMetadataPolicy(
      @JsonProperty("updateMode") UIUpdateMode updateMode,
      @JsonProperty("namesRefreshMillis") long namesRefreshMillis,
      @JsonProperty("authTTLMillis") long authTTLMillis,
      @JsonProperty("datasetDefinitionRefreshAfterMillis") long datasetDefinitionRefreshAfterMillis,
      @JsonProperty("datasetDefinitionExpireAfterMillis") long datasetDefinitionExpireAfterMillis) {
    super();
    this.updateMode = updateMode;
    this.namesRefreshMillis = namesRefreshMillis;
    this.authTTLMillis = authTTLMillis;
    this.datasetDefinitionRefreshAfterMillis = datasetDefinitionRefreshAfterMillis;
    this.datasetDefinitionExpireAfterMillis = datasetDefinitionExpireAfterMillis;
  }

  public static UIMetadataPolicy getDefaultUimetadataPolicy() {
    return DEFAULT_UIMETADATA_POLICY;
  }

  public UIUpdateMode getUpdateMode() {
    return updateMode;
  }

  public long getNamesRefreshMillis() {
    return namesRefreshMillis;
  }

  public long getAuthTTLMillis() {
    return authTTLMillis;
  }

  public long getDatasetDefinitionRefreshAfterMillis() {
    return datasetDefinitionRefreshAfterMillis;
  }

  public long getDatasetDefinitionExpireAfterMillis() {
    return datasetDefinitionExpireAfterMillis;
  }

  public static UIMetadataPolicy of(MetadataPolicy policy){
    if(policy == null){
      return DEFAULT_UIMETADATA_POLICY;
    }
    return new UIMetadataPolicy(
        UIUpdateMode.of(policy.getDatasetUpdateMode()),
        policy.getNamesRefreshMs(),
        policy.getAuthTtlMs(),
        policy.getDatasetDefinitionRefreshAfterMs(),
        policy.getDatasetDefinitionExpireAfterMs()
        );
  }

  public MetadataPolicy asMetadataPolicy(){
    return new MetadataPolicy()
        .setDatasetUpdateMode(updateMode.asUpdateMode())
        .setNamesRefreshMs(namesRefreshMillis)
        .setAuthTtlMs(authTTLMillis)
        .setDatasetDefinitionRefreshAfterMs(datasetDefinitionRefreshAfterMillis)
        .setDatasetDefinitionExpireAfterMs(datasetDefinitionExpireAfterMillis);
  }
}
